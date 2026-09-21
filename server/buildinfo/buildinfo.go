// Package buildinfo exposes diagnostic server build identity.
package buildinfo

import (
	"errors"
	"runtime"
	"runtime/debug"
)

const ServerVersion = "0.1.0-dev"

// ErrInvalidMetadata 不含原始构建输入。 / It never includes raw build input.
var ErrInvalidMetadata = errors.New("NIM_BUILD_INVALID_METADATA")

// Info 中缺失的来源信息保持未知。 / Missing provenance remains unknown.
type Info struct {
	ServerVersion string `json:"serverVersion"`
	GoVersion     string `json:"goVersion"`
	Revision      string `json:"sourceRevision,omitempty"`
	Modified      *bool  `json:"sourceModified,omitempty"`
}

// Read 读取嵌入信息，不访问 Git 或网络。 / Read uses embedded data, not Git or network I/O.
func Read() (Info, error) {
	info, _ := debug.ReadBuildInfo()
	return fromBuildInfo(info)
}

func fromBuildInfo(build *debug.BuildInfo) (Info, error) {
	info := Info{ServerVersion: ServerVersion, GoVersion: runtime.Version()}
	if build == nil {
		return info, nil
	}
	values := make(map[string]string)
	for _, setting := range build.Settings {
		switch setting.Key {
		case "vcs", "vcs.revision", "vcs.modified":
			if _, exists := values[setting.Key]; exists {
				return Info{}, ErrInvalidMetadata
			}
			values[setting.Key] = setting.Value
		}
	}
	vcs, hasVCS := values["vcs"]
	if len(values) != 0 && (!hasVCS || vcs != "git") {
		return Info{}, ErrInvalidMetadata
	}
	if revision, present := values["vcs.revision"]; present {
		if !validRevision(revision) {
			return Info{}, ErrInvalidMetadata
		}
		info.Revision = revision
	}
	if modified, present := values["vcs.modified"]; present {
		if modified != "true" && modified != "false" {
			return Info{}, ErrInvalidMetadata
		}
		value := modified == "true"
		info.Modified = &value
	}
	return info, nil
}

func validRevision(revision string) bool {
	if len(revision) != 40 && len(revision) != 64 {
		return false
	}
	for _, c := range []byte(revision) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
