package buildinfo

import (
	"errors"
	"runtime/debug"
	"strings"
	"testing"
)

func TestMissingProvenanceStaysUnknown(t *testing.T) {
	for _, build := range []*debug.BuildInfo{nil, {}, {Settings: []debug.BuildSetting{{Key: "vcs", Value: "git"}}}} {
		info, err := fromBuildInfo(build)
		if err != nil || info.Revision != "" || info.Modified != nil || info.GoVersion == "" {
			t.Fatalf("missing provenance became known: %+v, %v", info, err)
		}
	}
}

func TestGitIdentity(t *testing.T) {
	for _, length := range []int{40, 64} {
		for _, modified := range []string{"true", "false"} {
			revision := strings.Repeat("a", length)
			info, err := fromBuildInfo(&debug.BuildInfo{Settings: []debug.BuildSetting{
				{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: revision}, {Key: "vcs.modified", Value: modified},
				{Key: "unrelated", Value: "ignored"}, {Key: "unrelated", Value: "also ignored"},
			}})
			if err != nil || info.Revision != revision || info.Modified == nil || *info.Modified != (modified == "true") {
				t.Fatalf("identity mismatch: %+v, %v", info, err)
			}
		}
	}
}

func TestRejectInvalidMetadata(t *testing.T) {
	cases := [][]debug.BuildSetting{
		{{Key: "vcs", Value: "hg"}},
		{{Key: "vcs", Value: ""}},
		{{Key: "vcs.revision", Value: strings.Repeat("a", 40)}},
		{{Key: "vcs.modified", Value: "false"}},
	}
	for _, value := range []string{"", "TRUE", "False", "1", "0", "unknown", "false\n"} {
		cases = append(cases, []debug.BuildSetting{{Key: "vcs", Value: "git"}, {Key: "vcs.modified", Value: value}})
	}
	for _, key := range []string{"vcs", "vcs.revision", "vcs.modified"} {
		cases = append(cases, []debug.BuildSetting{{Key: key, Value: "git"}, {Key: key, Value: "git"}})
	}
	for _, settings := range cases {
		info, err := fromBuildInfo(&debug.BuildInfo{Settings: settings})
		if !errors.Is(err, ErrInvalidMetadata) || info != (Info{}) {
			t.Fatalf("invalid metadata returned usable identity: %+v, %v", info, err)
		}
	}
}

func TestRevisionBoundaries(t *testing.T) {
	for length := 0; length <= 80; length++ {
		if validRevision(strings.Repeat("a", length)) != (length == 40 || length == 64) {
			t.Fatalf("unexpected acceptance for length %d", length)
		}
	}
	for _, invalid := range []string{strings.Repeat("A", 40), strings.Repeat("g", 40), strings.Repeat("a", 39) + " ", strings.Repeat("a", 39) + "\n", strings.Repeat("a", 38) + "é"} {
		if validRevision(invalid) {
			t.Fatal("accepted invalid revision")
		}
		_, err := fromBuildInfo(&debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: invalid}}})
		if !errors.Is(err, ErrInvalidMetadata) {
			t.Fatal("metadata conversion did not reject invalid revision")
		}
	}
	if !validRevision("0123456789abcdef0123456789abcdef01234567") {
		t.Fatal("rejected lowercase hexadecimal revision")
	}
}
