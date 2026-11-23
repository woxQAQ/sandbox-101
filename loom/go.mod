module github.com/woxQAQ/haven/loom

go 1.26.5

require (
	github.com/erofs/go-erofs v0.3.3-0.20261101155608-4c135619312a
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.1.1
	github.com/woxQAQ/sandbox-protocol/gen v0.0.0
	golang.org/x/sys v0.49.0
	google.golang.org/protobuf v1.36.12
)

replace github.com/erofs/go-erofs => ../third_party/go-erofs

replace github.com/woxQAQ/sandbox-protocol/gen => ../protocol/gen
