package antenna

import (
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
)

const testAntenna = "ANT-01"

func newTestExecutor(t *testing.T) (*Executor, *scheduler.CommandQueue, *Fleet, store.Paths) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	queue, err := scheduler.NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := NewFleet(paths, nil, trail, []string{testAntenna})
	if err != nil {
		t.Fatal(err)
	}
	return NewExecutor(fleet, queue, trail), queue, fleet, paths
}

func queueDueCommands(t *testing.T, queue *scheduler.CommandQueue, passID string, revision int64, base time.Time, count int) []scheduler.PointingCommand {
	t.Helper()
	commands := make([]scheduler.PointingCommand, 0, count)
	for index := 0; index < count; index++ {
		commands = append(commands, scheduler.PointingCommand{
			AntennaID: testAntenna,
			ExecuteAt: base.Add(time.Duration(index) * time.Second),
			Azimuth:   100 + float64(index), Elevation: 10,
		})
	}
	if err := queue.Replace(passID, int64(revision), commands); err != nil {
		t.Fatal(err)
	}
	return queue.Due(base.Add(time.Hour))
}

func claimAntenna(fleet *Fleet, passID string, generation int64) {
	fleet.mu.Lock()
	defer fleet.mu.Unlock()
	state := fleet.states[testAntenna]
	state.OwnerPassID = passID
	state.OwnerGeneration = generation
	fleet.states[testAntenna] = state
	_ = fleet.flushLocked()
}

func releaseAntenna(fleet *Fleet, passID string, generation int64) {
	_ = fleet.Release(testAntenna, passID, generation)
}

func TestRunDueConfirmsExecutedCommandsAndDoesNotRedrive(t *testing.T) {
	executor, queue, fleet, paths := newTestExecutor(t)
	now := time.Now().UTC()
	commands := queueDueCommands(t, queue, "P1", 1, now.Add(-time.Minute), 3)
	claimAntenna(fleet, "P1", 1)

	result, err := executor.RunDue(now)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if len(result.Executed) != 3 || result.Failed() || len(result.Retry) != 0 || len(result.Retired) != 0 {
		t.Fatalf("unexpected first result: %#v", result)
	}

	// A second run must find nothing due: confirmed commands stay in the queue
	// but are never handed out again.
	result, err = executor.RunDue(now)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if len(result.Executed) != 0 || len(result.Retry) != 0 || len(result.Retired) != 0 {
		t.Fatalf("confirmed commands were handed out again: %#v", result)
	}
	for _, command := range commands {
		stored, found := queue.Get(command.ID)
		if !found || !stored.Executed() {
			t.Fatalf("command %s was not durably confirmed", command.ID)
		}
	}

	// The confirmation survives a process restart: a queue reloaded from disk
	// must not redrive the commands either.
	reloaded, err := scheduler.NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	reloadedFleet, err := NewFleet(paths, nil, audit.NewTrail(store.NewAppendLog(paths)), []string{testAntenna})
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewExecutor(reloadedFleet, reloaded, audit.NewTrail(store.NewAppendLog(paths)))
	result, err = restarted.RunDue(now)
	if err != nil {
		t.Fatalf("post-restart run failed: %v", err)
	}
	if len(result.Executed) != 0 {
		t.Fatalf("commands re-driven after restart: %#v", result.Executed)
	}
}

func TestRunDueClassifiesFailureRetryAndStopsRepeatingCompletedCommands(t *testing.T) {
	executor, queue, fleet, _ := newTestExecutor(t)
	now := time.Now().UTC()
	commands := queueDueCommands(t, queue, "P1", 1, now.Add(-time.Minute), 4)
	claimAntenna(fleet, "P1", 1)

	// Lose antenna ownership between the second and third command so the third
	// hardware drive fails.
	seen := 0
	executor.beforeExecute = func(command scheduler.PointingCommand) {
		seen++
		if seen == 3 {
			releaseAntenna(fleet, "P1", 1)
		}
	}

	result, err := executor.RunDue(now)
	if err == nil {
		t.Fatal("expected RunDue to report the failed command")
	}
	if len(result.Executed) != 2 || result.Executed[0].ID != commands[0].ID || result.Executed[1].ID != commands[1].ID {
		t.Fatalf("executed commands misclassified: %#v", result.Executed)
	}
	if result.Failure == nil || result.Failure.Command.ID != commands[2].ID || result.Failure.Error == "" {
		t.Fatalf("failed command misclassified: %#v", result.Failure)
	}
	if len(result.Retry) != 1 || result.Retry[0].ID != commands[3].ID {
		t.Fatalf("unexecuted commands misclassified as retry: %#v", result.Retry)
	}
	if len(result.Retired) != 0 {
		t.Fatalf("unexpected retired commands: %#v", result.Retired)
	}

	// Restore ownership and run again: the failed command and the one after it
	// are retried; the two confirmed commands are not re-driven.
	executor.beforeExecute = nil
	claimAntenna(fleet, "P1", 2)
	result, err = executor.RunDue(now.Add(time.Second))
	if err != nil {
		t.Fatalf("retry run failed: %v", err)
	}
	if len(result.Executed) != 2 {
		t.Fatalf("expected failed and pending command to run once each, got %#v", result.Executed)
	}
	ids := map[string]bool{result.Executed[0].ID: true, result.Executed[1].ID: true}
	if !ids[commands[2].ID] || !ids[commands[3].ID] {
		t.Fatalf("retry executed the wrong commands: %#v", result.Executed)
	}
}

func TestRunDueClassifiesCommandsRetiredAfterSnapshot(t *testing.T) {
	executor, queue, fleet, _ := newTestExecutor(t)
	now := time.Now().UTC()
	commands := queueDueCommands(t, queue, "P1", 1, now.Add(-time.Minute), 3)
	claimAntenna(fleet, "P1", 1)

	// After the first command is driven, a revision replacement retires the
	// remaining revision-1 commands before their turn.
	seen := 0
	executor.beforeExecute = func(command scheduler.PointingCommand) {
		seen++
		if seen == 2 {
			replacement, err := BuildCommands("P1", testAntenna, 2, []TrackPoint{
				{At: now.Add(time.Hour), Azimuth: 110, Elevation: 12},
				{At: now.Add(2 * time.Hour), Azimuth: 120, Elevation: 14},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := queue.Replace("P1", 2, replacement); err != nil {
				t.Fatal(err)
			}
		}
	}

	result, err := executor.RunDue(now)
	if err != nil {
		t.Fatalf("retirement is not a failure: %v", err)
	}
	if len(result.Executed) != 1 || result.Executed[0].ID != commands[0].ID {
		t.Fatalf("executed bucket wrong: %#v", result.Executed)
	}
	if len(result.Retired) != 2 {
		t.Fatalf("expected two retired commands, got %#v", result.Retired)
	}
	retired := map[string]bool{result.Retired[0].ID: true, result.Retired[1].ID: true}
	if !retired[commands[1].ID] || !retired[commands[2].ID] {
		t.Fatalf("wrong commands retired: %#v", result.Retired)
	}
	if len(result.Retry) != 0 || result.Failed() {
		t.Fatalf("retired commands must not be retried: %#v", result)
	}

	// The revision-2 replacement remains queued for the future and was not driven.
	state, _ := fleet.State(testAntenna)
	if state.Azimuth != commands[0].Azimuth {
		t.Fatalf("replacement command leaked to hardware: az=%.2f", state.Azimuth)
	}
}
