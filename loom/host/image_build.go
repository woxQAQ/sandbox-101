package host

import (
	"errors"
	"os"
	"path/filepath"
)

// LayerCacheRequest binds one preflight-validated OCI layer and its byte/entry
// counts to the exact builder whose immutable cache will be published.
type LayerCacheRequest struct {
	Input   LayerInput
	Builder string
	Bytes   int64
	Entries int64
}

// FsmetaLayers fixes the ordered immutable devices and the private output
// directory for one metadata image. Builder and manifest identities remain
// separate inputs to their respective materializers.
type FsmetaLayers struct {
	StateRoot string
	Output    string
	Artifacts []ImageArtifact
}

// LayerArtifactIdentity authenticates one external data device and its receipt.
// Device order is assigned separately when constructing an image manifest.
type LayerArtifactIdentity struct {
	Builder     string
	DiffID      string
	SHA256      string
	CacheSHA256 string
}

// publishLayerCache seals one builder's completed private stage before publishing
// its immutable directory. Both builders retain the same durability boundary.
func publishLayerCache(state, stage string, cached layerCache) (layerCache, error) {
	var err error
	cached.SHA256, err = digestFile(filepath.Join(stage, "layer.erofs"))
	if err != nil {
		return cached, err
	}
	if err = writeJSON(filepath.Join(stage, "layer.json"), cached); err != nil {
		return cached, err
	}
	if err = sealTree(stage); err != nil {
		return cached, err
	}
	if err = syncTree(stage); err != nil {
		return cached, err
	}
	parent := filepath.Join(state, "layer-cache", cached.Builder[7:])
	dest := filepath.Join(parent, cached.DiffID[7:])
	if _, err = os.Lstat(dest); !os.IsNotExist(err) {
		return cached, errors.New("layer cache destination already exists")
	}
	if err = os.Rename(stage, dest); err != nil {
		return cached, err
	}
	for _, dir := range []string{
		parent,
		filepath.Dir(parent),
		state,
	} {
		if err = syncDir(dir); err != nil {
			return cached, err
		}
	}
	return loadLayerCache(state, cached.Builder, cached.DiffID)
}
