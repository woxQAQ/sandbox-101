// Package image owns immutable image catalog and cache custody facts.
package image

import "github.com/woxQAQ/haven/loom/imageimport"

// Cache preserves the returned physical witness and its original funding operation.
type Cache struct {
	Witness         imageimport.ImageCacheWitness `json:"witness"`
	SourceOperation string                        `json:"source_operation"`
}

type CacheKey struct {
	Builder string
	DiffID  string
}
