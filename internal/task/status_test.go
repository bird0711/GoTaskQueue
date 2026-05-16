package task

import "testing"

func TestStatusIsValid(t *testing.T) {
	validStatuses := []Status{
		StatusScheduled,
		StatusPending,
		StatusRunning,
		StatusSuccess,
		StatusFailed,
		StatusRetrying,
		StatusDead,
	}

	for _, status := range validStatuses {
		if !status.IsValid() {
			t.Fatalf("expected %q to be valid", status)
		}
	}

	if Status("unknown").IsValid() {
		t.Fatal("expected unknown status to be invalid")
	}
}

func TestCanTransition(t *testing.T) {
	allowed := [][2]Status{
		{StatusScheduled, StatusPending},
		{StatusPending, StatusRunning},
		{StatusRunning, StatusSuccess},
		{StatusRunning, StatusFailed},
		{StatusRunning, StatusRetrying},
		{StatusRunning, StatusDead},
		{StatusFailed, StatusRetrying},
		{StatusFailed, StatusDead},
		{StatusRetrying, StatusPending},
		{StatusRetrying, StatusRunning},
	}

	for _, transition := range allowed {
		if !CanTransition(transition[0], transition[1]) {
			t.Fatalf("expected transition %q -> %q to be allowed", transition[0], transition[1])
		}
	}

	denied := [][2]Status{
		{StatusScheduled, StatusRunning},
		{StatusPending, StatusSuccess},
		{StatusSuccess, StatusRunning},
		{StatusDead, StatusPending},
		{Status("unknown"), StatusPending},
	}

	for _, transition := range denied {
		if CanTransition(transition[0], transition[1]) {
			t.Fatalf("expected transition %q -> %q to be denied", transition[0], transition[1])
		}
	}
}

func TestStatusIsTerminal(t *testing.T) {
	if !StatusSuccess.IsTerminal() {
		t.Fatal("expected success to be terminal")
	}
	if !StatusDead.IsTerminal() {
		t.Fatal("expected dead to be terminal")
	}
	if StatusRunning.IsTerminal() {
		t.Fatal("expected running to be non-terminal")
	}
}
