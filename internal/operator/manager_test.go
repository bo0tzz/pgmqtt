package operator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveLeaderElectionNamespace(t *testing.T) {
	t.Run("explicit wins", func(t *testing.T) {
		got := resolveLeaderElectionNamespace("mqtt-prod")
		if got != "mqtt-prod" {
			t.Fatalf("explicit not honored: got %q", got)
		}
	})

	t.Run("falls back to file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "namespace")
		if err := os.WriteFile(path, []byte("from-file\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		prev := inClusterNamespacePath
		inClusterNamespacePath = path
		defer func() { inClusterNamespacePath = prev }()

		got := resolveLeaderElectionNamespace("")
		if got != "from-file" {
			t.Fatalf("file fallback not honored: got %q", got)
		}
	})

	t.Run("empty when neither resolves", func(t *testing.T) {
		prev := inClusterNamespacePath
		inClusterNamespacePath = filepath.Join(t.TempDir(), "missing")
		defer func() { inClusterNamespacePath = prev }()

		got := resolveLeaderElectionNamespace("")
		if got != "" {
			t.Fatalf("expected empty, got %q", got)
		}
	})
}

