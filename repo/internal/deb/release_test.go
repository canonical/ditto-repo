package deb

import "testing"

func TestParseRelease(t *testing.T) {
	releaseContent := `Origin: Ubuntu
Label: Ubuntu
Suite: focal
Version: 20.04
Codename: focal
Date: Thu, 23 Apr 2020 17:33:17 UTC
Architectures: amd64 arm64 armhf i386 ppc64el riscv64 s390x
Components: main restricted universe multiverse
Description: Ubuntu Focal 20.04
MD5Sum:
 d41d8cd98f00b204e9800998ecf8427e        0 main/binary-amd64/Packages
SHA256:
 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855        0 main/binary-amd64/Packages.gz
 abc1234567890abcdef1234567890abcdef1234567890abcdef1234567890abc    12345 main/binary-arm64/Packages.gz
 def9876543210fedcba9876543210fedcba9876543210fedcba9876543210fed     5678 universe/binary-amd64/Packages.xz
 123abc456def789012abc456def789012abc456def789012abc456def789012a     1000 main/i18n/Translation-en.gz
 456def789abc012345def789abc012345def789abc012345def789abc012345d     2000 main/i18n/Translation-es.bz2
 789012345678901234567890123456789012345678901234567890123456789     3000 contrib/binary-amd64/Packages.gz
 aaa111bbb222ccc333ddd444eee555fff666aaa111bbb222ccc333ddd444eee5     4000 main/cnf/Commands-amd64.xz
 bbb222ccc333ddd444eee555fff666aaa111bbb222ccc333ddd444eee555fff6     5000 main/cnf/Commands-arm64.xz
 ccc333ddd444eee555fff666aaa111bbb222ccc333ddd444eee555fff666aaa1     6000 universe/cnf/Commands-amd64.xz
 ddd444eee555fff666aaa111bbb222ccc333ddd444eee555fff666aaa111bbb2     7000 main/dep11/Components-amd64.yml.gz
 eee555fff666aaa111bbb222ccc333ddd444eee555fff666aaa111bbb222ccc3     8000 main/dep11/Components-arm64.yml.xz
 fff666aaa111bbb222ccc333ddd444eee555fff666aaa111bbb222ccc333ddd4     9000 main/dep11/Components-i386.yml.gz
 111222333444555666777888999000111222333444555666777888999000111    10000 main/dep11/icons-64x64.tar.gz
 222333444555666777888999000111222333444555666777888999000111222    11000 contrib/dep11/Components-amd64.yml.gz`

	indices := ParseRelease(releaseContent)
	indexPaths := make([]string, len(indices))
	for i, index := range indices {
		indexPaths[i] = index.Path
	}

	expected := []string{
		"main/binary-amd64/Packages.gz",
		"main/binary-arm64/Packages.gz",
		"universe/binary-amd64/Packages.xz",
		"main/i18n/Translation-en.gz",
		"main/i18n/Translation-es.bz2",
		"contrib/binary-amd64/Packages.gz",
		"main/cnf/Commands-amd64.xz",
		"main/cnf/Commands-arm64.xz",
		"universe/cnf/Commands-amd64.xz",
		"main/dep11/Components-amd64.yml.gz",
		"main/dep11/Components-arm64.yml.xz",
		"main/dep11/Components-i386.yml.gz",
		"main/dep11/icons-64x64.tar.gz",
		"contrib/dep11/Components-amd64.yml.gz",
	}

	if len(indices) != len(expected) {
		t.Errorf("expected %d indices, got %d", len(expected), len(indices))
	}

	for i, idx := range indexPaths {
		if idx != expected[i] {
			t.Errorf("index %d: expected %s, got %s", i, expected[i], idx)
		}
	}
}

func TestParseRelease_EmptyContent(t *testing.T) {
	indices := ParseRelease("")

	if len(indices) != 0 {
		t.Errorf("expected 0 indices from emtpy content, got %d", len(indices))
	}
}

func TestParseRelease_NoSHA256Block(t *testing.T) {
	releaseContent := `Origin: Ubuntu
Label: Ubuntu
Suite: focal
MD5Sum:
 d41d8cd98f00b204e9800998ecf8427e        0 main/binary-amd64/Packages`

	indices := ParseRelease(releaseContent)

	if len(indices) != 0 {
		t.Errorf("expected 0 indices without SHA256 block, got %d", len(indices))
	}
}

func TestMatches(t *testing.T) {
	f := SelectionFilter{
		Components: []string{"main", "universe"},
		Archs:      []string{"amd64", "arm64"},
		Languages:  []string{"en"},
	}

	tests := []struct {
		name     string
		filePath string
		want     bool
	}{
		{
			name:     "main binary amd64 packages",
			filePath: "main/binary-amd64/Packages.gz",
			want:     true,
		},
		{
			name:     "universe binary arm64 packages",
			filePath: "universe/binary-arm64/Packages.xz",
			want:     true,
		},
		{
			name:     "main translation en",
			filePath: "main/i18n/Translation-en.gz",
			want:     true,
		},
		{
			name:     "wrong component",
			filePath: "contrib/binary-amd64/Packages.gz",
			want:     false,
		},
		{
			name:     "wrong architecture",
			filePath: "main/binary-i386/Packages.gz",
			want:     false,
		},
		{
			name:     "wrong language",
			filePath: "main/i18n/Translation-es.gz",
			want:     false,
		},
		{
			name:     "not a package or translation file",
			filePath: "main/source/Sources.gz",
			want:     false,
		},
		{
			name:     "main cnf commands amd64",
			filePath: "main/cnf/Commands-amd64.xz",
			want:     true,
		},
		{
			name:     "universe cnf commands arm64",
			filePath: "universe/cnf/Commands-arm64.xz",
			want:     true,
		},
		{
			name:     "wrong cnf architecture",
			filePath: "main/cnf/Commands-i386.xz",
			want:     false,
		},
		{
			name:     "wrong component for cnf",
			filePath: "contrib/cnf/Commands-amd64.xz",
			want:     false,
		},
		{
			name:     "main dep11 components amd64",
			filePath: "main/dep11/Components-amd64.yml.gz",
			want:     true,
		},
		{
			name:     "universe dep11 components arm64 xz",
			filePath: "universe/dep11/Components-arm64.yml.xz",
			want:     true,
		},
		{
			name:     "dep11 icons not arch specific",
			filePath: "main/dep11/icons-64x64.tar.gz",
			want:     true,
		},
		{
			name:     "wrong dep11 architecture",
			filePath: "main/dep11/Components-i386.yml.gz",
			want:     false,
		},
		{
			name:     "wrong component for dep11",
			filePath: "contrib/dep11/Components-amd64.yml.gz",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := f.Matches(tt.filePath)
			if got != tt.want {
				t.Errorf("Matches(%s) = %v, want %v", tt.filePath, got, tt.want)
			}
		})
	}
}

func TestMatches_EmptyFilter(t *testing.T) {
	f := SelectionFilter{
		Components: []string{},
		Archs:      []string{},
		Languages:  []string{},
	}

	tests := []string{
		"main/binary-amd64/Packages.gz",
		"universe/i18n/Translation-en.gz",
		"contrib/binary-arm64/Packages.xz",
	}

	for _, filePath := range tests {
		if f.Matches(filePath) {
			t.Errorf("expected Matches(%s) = false with empty config", filePath)
		}
	}
}
