package host

import "github.com/woxQAQ/haven/loom"

func syncDir(path string) error                              { return loom.SyncDir(path) }
func writeJSON(path string, value any) error                 { return loom.WriteJSON(path, value) }
func readJSON(path string, value any) error                  { return loom.ReadJSON(path, value) }
func digestFile(path string) (string, error)                 { return loom.DigestFile(path) }
func regular(path string) error                              { return loom.Regular(path) }
func syncTree(root string) error                             { return loom.SyncTree(root) }
func sealTree(root string) error                             { return loom.SealTree(root) }
func verifyFiles(root string, files map[string]string) error { return loom.VerifyFiles(root, files) }
