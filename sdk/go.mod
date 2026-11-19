module github.com/woxQAQ/haven/sdk

go 1.26.5

require (
	github.com/woxQAQ/haven/types v0.0.0
	github.com/woxQAQ/sandbox-protocol/gen v0.0.0
	google.golang.org/protobuf v1.36.12
)

replace github.com/woxQAQ/haven/types => ../types

replace github.com/woxQAQ/sandbox-protocol/gen => ../protocol/gen
