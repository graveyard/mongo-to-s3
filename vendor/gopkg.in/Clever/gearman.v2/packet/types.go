package packet

// Type represents the type of the Gearman packet
type Type int

const (
	// PreSleep = PRE_SLEEP
	PreSleep = 4
	// SubmitJob = SUBMIT_JOB
	SubmitJob Type = 7
	// JobCreated = JOB_CREATED
	JobCreated = 8
	// JobAssign = JOB_ASSIGN
	JobAssign = 11
	// WorkStatus = WORK_STATUS
	WorkStatus = 12
	// WorkComplete = WORK_COMPLETE
	WorkComplete = 13
	// WorkFail = WORK_FAIL
	WorkFail = 14
	// SubmitJobBg = SUBMIT_JOB_BG
	SubmitJobBg = 18
	// WorkData = WORK_DATA
	WorkData = 28
	// WorkWarning = WORK_WARNING
	WorkWarning = 29
)
