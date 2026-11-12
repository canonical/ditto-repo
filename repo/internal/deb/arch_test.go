package deb

import "testing"

func TestArchOf(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		archs []string
		want  string
	}{
		{
			name:  "binary index directory",
			path:  "dists/stable/main/binary-amd64/Packages.gz",
			archs: []string{"amd64"},
			want:  "amd64",
		},
		{
			name:  "command-not-found file",
			path:  "dists/stable/main/Commands-amd64.xz",
			archs: []string{"amd64"},
			want:  "amd64",
		},
		{
			name:  "DEP-11 components file",
			path:  "dists/stable/main/dep11/Components-amd64.yml.gz",
			archs: []string{"amd64"},
			want:  "amd64",
		},
		{
			name:  "package filename",
			path:  "pool/main/h/hello/hello_2.10-2_amd64.deb",
			archs: []string{"amd64"},
			want:  "amd64",
		},
		{
			name:  "first matching arch is returned",
			path:  "dists/stable/main/binary-arm64/Packages.gz",
			archs: []string{"amd64", "arm64"},
			want:  "arm64",
		},
		{
			name:  "empty arch entries are skipped",
			path:  "dists/stable/main/binary-amd64/Packages.gz",
			archs: []string{"", "amd64"},
			want:  "amd64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ArchOf(tt.path, tt.archs); got != tt.want {
				t.Errorf("ArchOf(%q, %q) = %q, want %q", tt.path, tt.archs, got, tt.want)
			}
		})
	}
}

func TestArchOf_NoMatches(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		archs []string
	}{
		{
			name:  "no architectures",
			path:  "dists/stable/main/binary-amd64/Packages.gz",
			archs: nil,
		},
		{
			name:  "path not architecture-specific",
			path:  "dists/stable/main/source/Sources.gz",
			archs: []string{"amd64"},
		},
		{
			name:  "arch not present in path",
			path:  "dists/stable/main/binary-i386/Packages.gz",
			archs: []string{"amd64"},
		},
		{
			name:  "amd64 does not match amd64v3",
			path:  "dists/stable/main/binary-amd64v3/Packages.gz",
			archs: []string{"amd64"},
		},
		{
			name:  "empty arch alone does not match",
			path:  "pool/main/x/x_.deb",
			archs: []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ArchOf(tt.path, tt.archs); got != "" {
				t.Errorf("ArchOf(%q, %q) = %q, want \"\"", tt.path, tt.archs, got)
			}
		})
	}
}
