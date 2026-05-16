package task

type Status string

const (
	StatusScheduled Status = "scheduled"
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSuccess   Status = "success"
	StatusFailed    Status = "failed"
	StatusRetrying  Status = "retrying"
	StatusDead      Status = "dead"
)

var validTransitions = map[Status]map[Status]struct{}{
	StatusScheduled: {
		StatusPending: {},
	},
	StatusPending: {
		StatusRunning: {},
	},
	StatusRunning: {
		StatusSuccess:  {},
		StatusFailed:   {},
		StatusRetrying: {},
		StatusDead:     {},
	},
	StatusFailed: {
		StatusRetrying: {},
		StatusDead:     {},
	},
	StatusRetrying: {
		StatusPending: {},
		StatusRunning: {},
	},
}

func (s Status) IsValid() bool {
	switch s {
	case StatusScheduled, StatusPending, StatusRunning, StatusSuccess, StatusFailed, StatusRetrying, StatusDead:
		return true
	default:
		return false
	}
}

func (s Status) IsTerminal() bool {
	return s == StatusSuccess || s == StatusDead
}

func CanTransition(from, to Status) bool {
	nextStatuses, ok := validTransitions[from]
	if !ok {
		return false
	}

	_, ok = nextStatuses[to]
	return ok
}
