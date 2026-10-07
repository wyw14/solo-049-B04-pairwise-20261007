package antenna

import (
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

func TestRunDueAcknowledgesExecutedCommands(t *testing.T) {
	executor, queue, fleet, paths := newTestExecutor(t)
	claimAntenna(t, fleet, "PASS-1", "ANT-01")
	now := time.Now().UTC()
	queued := queueRevision(t, queue, "PASS-1", 1, []scheduler.PointingCommand{
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-3 * time.Second), Azimuth: 10, Elevation: 20},
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-2 * time.Second), Azimuth: 20, Elevation: 30},
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-time.Second), Azimuth: 30, Elevation: 40},
		{AntennaID: "ANT-01", ExecuteAt: now.Add(time.Hour), Azimuth: 40, Elevation: 50},
	})
	result, err := executor.RunDue(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Executed) != 3 || len(result.PendingRetry) != 0 || len(result.Retired) != 0 {
		t.Fatalf("unexpected run result: %#v", result)
	}
	for index, command := range result.Executed {
		if command.ID != queued[index].ID {
			t.Fatalf("execution order drifted at %d: %s != %s", index, command.ID, queued[index].ID)
		}
		if !command.Executed || command.ExecutedAt.IsZero() {
			t.Fatalf("executed command %s lacks acknowledgment: %#v", command.ID, command)
		}
	}
	state, _ := fleet.State("ANT-01")
	if state.Azimuth != 30 || state.Elevation != 40 || state.Mode != Tracking {
		t.Fatalf("antenna did not land on the last due command: %#v", state)
	}
	again, err := executor.RunDue(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Executed) != 0 || len(again.PendingRetry) != 0 || len(again.Retired) != 0 {
		t.Fatalf("acknowledged commands were executed again: %#v", again)
	}
	reloaded, err := scheduler.NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	if due := reloaded.Due(now); len(due) != 0 {
		t.Fatalf("acknowledged commands are still due after reload: %#v", due)
	}
}

func TestRunDueFailureMarksExecutedAndPendingRetry(t *testing.T) {
	executor, queue, fleet, _ := newTestExecutor(t)
	claimAntenna(t, fleet, "PASS-1", "ANT-01")
	now := time.Now().UTC()
	queued := queueRevision(t, queue, "PASS-1", 1, []scheduler.PointingCommand{
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-3 * time.Second), Azimuth: 10, Elevation: 20},
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-2 * time.Second), Azimuth: 20, Elevation: 95},
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-time.Second), Azimuth: 30, Elevation: 40},
	})
	result, err := executor.RunDue(now)
	if err == nil {
		t.Fatal("expected the out-of-limits elevation to fail the run")
	}
	if len(result.Executed) != 1 || result.Executed[0].ID != queued[0].ID {
		t.Fatalf("executed prefix mismatch: %#v", result.Executed)
	}
	if len(result.PendingRetry) != 2 || result.PendingRetry[0].ID != queued[1].ID || result.PendingRetry[1].ID != queued[2].ID {
		t.Fatalf("pending retry mismatch: %#v", result.PendingRetry)
	}
	if len(result.Retired) != 0 {
		t.Fatalf("no command should be retired: %#v", result.Retired)
	}
	if result.Failure == "" {
		t.Fatal("failure reason was not recorded")
	}
	if due := queue.Due(now); len(due) != 2 || due[0].ID != queued[1].ID || due[1].ID != queued[2].ID {
		t.Fatalf("failed and unattempted commands must stay due: %#v", due)
	}
	retry, err := executor.RunDue(now)
	if err == nil {
		t.Fatal("expected the retry to fail on the same command")
	}
	if len(retry.Executed) != 0 {
		t.Fatalf("retry re-executed acknowledged commands: %#v", retry.Executed)
	}
	if len(retry.PendingRetry) != 2 || retry.PendingRetry[0].ID != queued[1].ID {
		t.Fatalf("retry pending mismatch: %#v", retry.PendingRetry)
	}
	state, _ := fleet.State("ANT-01")
	if state.Azimuth != 10 || state.Elevation != 20 {
		t.Fatalf("antenna moved past the acknowledged prefix: %#v", state)
	}
	fixed := queueRevision(t, queue, "PASS-1", 2, []scheduler.PointingCommand{
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-2 * time.Second), Azimuth: 20, Elevation: 45},
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-time.Second), Azimuth: 30, Elevation: 40},
	})
	healed, err := executor.RunDue(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(healed.Executed) != 2 || healed.Executed[0].ID != fixed[0].ID || healed.Executed[1].ID != fixed[1].ID {
		t.Fatalf("corrected revision did not run: %#v", healed)
	}
	if due := queue.Due(now); len(due) != 0 {
		t.Fatalf("queue still holds due commands: %#v", due)
	}
}

func TestRunCommandsMarksSupersededCommandsRetired(t *testing.T) {
	executor, queue, fleet, _ := newTestExecutor(t)
	claimAntenna(t, fleet, "PASS-1", "ANT-01")
	now := time.Now().UTC()
	queueRevision(t, queue, "PASS-1", 1, []scheduler.PointingCommand{
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-2 * time.Second), Azimuth: 10, Elevation: 20},
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-time.Second), Azimuth: 20, Elevation: 30},
	})
	snapshot := queue.Due(now)
	current := queueRevision(t, queue, "PASS-1", 2, []scheduler.PointingCommand{
		{AntennaID: "ANT-01", ExecuteAt: now.Add(-time.Second), Azimuth: 50, Elevation: 60},
	})
	// The replan lands after the due snapshot but before the executor drains it.
	result, err := executor.runCommands(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Executed) != 0 || len(result.PendingRetry) != 0 {
		t.Fatalf("superseded commands must not run: %#v", result)
	}
	if len(result.Retired) != 2 || result.Retired[0].ID != snapshot[0].ID || result.Retired[1].ID != snapshot[1].ID {
		t.Fatalf("superseded commands were not marked retired: %#v", result.Retired)
	}
	followup, err := executor.RunDue(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(followup.Executed) != 1 || followup.Executed[0].ID != current[0].ID {
		t.Fatalf("replacement revision did not run: %#v", followup)
	}
}

func newTestExecutor(t *testing.T) (*Executor, *scheduler.CommandQueue, *Fleet, store.Paths) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	timeline, err := scheduler.NewTimeline(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := NewFleet(paths, timeline, trail, []string{"ANT-01"})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := scheduler.NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	return NewExecutor(fleet, queue, trail), queue, fleet, paths
}

func claimAntenna(t *testing.T, fleet *Fleet, passID, antennaID string) {
	t.Helper()
	now := time.Now().UTC()
	_, err := fleet.Claim(scheduler.Interval{
		ReservationID: "RES-1", PassID: passID, AntennaID: antennaID,
		Start: now, End: now.Add(time.Minute), Generation: 1, Status: scheduler.Reserved,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func queueRevision(t *testing.T, queue *scheduler.CommandQueue, passID string, revision int64, commands []scheduler.PointingCommand) []scheduler.PointingCommand {
	t.Helper()
	if err := queue.Replace(passID, revision, commands); err != nil {
		t.Fatal(err)
	}
	var queued []scheduler.PointingCommand
	for _, command := range queue.List(passID) {
		if command.Revision == revision {
			queued = append(queued, command)
		}
	}
	return queued
}
