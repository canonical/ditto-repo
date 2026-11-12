package deb

import (
	"slices"
	"testing"
)

func TestCandidateURLs_Preferred(t *testing.T) {
	archive := "https://archive.ubuntu.com/ubuntu"
	ports := "https://ports.ubuntu.com/ubuntu"
	bases := []string{archive, ports}

	got := CandidateURLs(bases, ports)
	want := []string{ports, archive}
	if !slices.Equal(got, want) {
		t.Errorf("CandidateURLs = %v, want %v", got, want)
	}
}

func TestCandidateURLs_NoPreferences(t *testing.T) {
	archive := "https://archive.ubuntu.com/ubuntu"
	security := "https://security.ubuntu.com/ubuntu"
	bases := []string{archive, security, archive, ""}

	got := CandidateURLs(bases, "")
	want := []string{archive, security}
	if !slices.Equal(got, want) {
		t.Errorf("CandidateURLs = %v, want %v", got, want)
	}
}

func TestCandidateURLs_SingleURL(t *testing.T) {
	archive := "https://archive.ubuntu.com/ubuntu"
	bases := []string{archive}

	for _, preferred := range []string{"", archive} {
		got := CandidateURLs(bases, preferred)
		want := []string{archive}
		if !slices.Equal(got, want) {
			t.Errorf("CandidateURLs(%v, %q) = %v, want %v", bases, preferred, got, want)
		}
	}
}
