# go-erofs dependency provenance

This directory contains the production Go files and MIT license from
[erofs/go-erofs](https://github.com/erofs/go-erofs), commit
[`4c135619312a35b3012c2fc57e2e8803274ea1ec`](https://github.com/erofs/go-erofs/tree/4c135619312a35b3012c2fc57e2e8803274ea1ec)
(module version `v0.3.3-0.20261101155608-4c135619312a`). The implementation
is used as a dependency: the engine delegates filesystem metadata, inode,
directory, xattr and chunk-index encoding to its public Writer API. Source
archives/checkouts used during investigation were kept outside this directory.

One local patch is necessary for compatibility with the kernel and native
erofs-utils 1.9.4. In `writer.go`, the device-table slot offset was calculated
as `SizeSuperBlock / 16` (8), while slots are written after the superblock at
byte 1152 (slot 9). The corrected expression is:

```go
devtSlotOff = uint16((disk.SuperBlockOffset + disk.SizeSuperBlock) / disk.SizeDeviceSlot)
```

This fixes the dependency at its encoding source. Engine code does not patch
serialized EROFS bytes. The exact patched dependency identity is recorded as
`github.com/erofs/go-erofs@v0.3.3-0.20261101155608-4c135619312a+sandbox-devtable-v1`
in the immutable image builder profile. No previously published profile uses
this identity.

`TestActualLayeredEROFSImportAndExtraction` and
`TestActualSharedOCIHardlinksAndMetadata` check the table with the engine's
bounded reader, then require native fsck to read every file through all external
devices. `TestActualLayerOrderAndBackendLimit` also covers 16 device slots.
The original unpatched offset fails the table check before publication.
