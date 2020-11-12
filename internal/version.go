package internal

import "regexp"

var GitVersion string = "unknown"

var reVersion = regexp.MustCompile(`^(v\d+\.\d+.\d+)(?:-)?(.+)?$`)

// String formats the version in semver format, see semver.org
func Version() string {
	m := reVersion.FindStringSubmatch(GitVersion)
	if m == nil || len(m) < 3 {
		return "v0.0.0+" + GitVersion
	}

	if m[2] == "" {
		return m[1]
	}
	return m[1] + "+" + m[2]
}
