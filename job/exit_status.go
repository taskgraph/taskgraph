package job

const (
	SuccessStatus ExitStatus = iota + 1
	FailureStatus
)

type ExitStatus int
