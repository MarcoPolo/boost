package build

var CurrentCommit string

const BuildVersion = "1.6.0-rc3"

func UserVersion() string {
	return BuildVersion + CurrentCommit
}
