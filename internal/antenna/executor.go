package antenna

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/scheduler"
)

// ErrCommandRetired signals that a due command belonged to a revision that was
// retired before the executor could drive it. Such commands are reported as
// retired rather than as failures.
var ErrCommandRetired = errors.New("pointing command belongs to a retired revision")

type Executor struct {
	fleet *Fleet
	queue *scheduler.CommandQueue
	audit *audit.Trail

	// runMu serializes due runs so two overlapping ticks cannot each pass the
	// executability check and drive the same command twice.
	runMu sync.Mutex

	// beforeExecute runs after the due snapshot is taken and immediately before
	// a command is driven. It is nil in production and exists to make the
	// snapshot/execution boundary deterministic in tests.
	beforeExecute func(scheduler.PointingCommand)
}

func NewExecutor(fleet *Fleet, queue *scheduler.CommandQueue, trail *audit.Trail) *Executor {
	return &Executor{fleet: fleet, queue: queue, audit: trail}
}

// CommandFailure pairs the command that stopped a run with the hardware error.
type CommandFailure struct {
	Command scheduler.PointingCommand `json:"command"`
	Error   string                    `json:"error"`
}

// RunResult classifies every due command a run considered.
//
//   - Executed: commands driven to the antenna and durably confirmed; they are
//     never handed out again, even after a process restart.
//   - Failure:  the command whose execution failed and stopped the run, if any.
//   - Retry:    commands still due that were never attempted and must be retried.
//   - Retired:  commands that were due when the run started but were retired by
//     a revision replacement before their turn.
type RunResult struct {
	Executed []scheduler.PointingCommand `json:"executed"`
	Failure  *CommandFailure             `json:"failure,omitempty"`
	Retry    []scheduler.PointingCommand `json:"retry"`
	Retired  []scheduler.PointingCommand `json:"retired"`
}

func (r RunResult) Failed() bool {
	return r.Failure != nil
}

func (r RunResult) err() error {
	if r.Failure == nil {
		return nil
	}
	return fmt.Errorf("pointing command %s failed: %s", r.Failure.Command.ID, r.Failure.Error)
}

// RunDue drives due commands in ExecuteAt order. Every successfully driven
// command is confirmed with the queue before the next one is attempted, so a
// mid-run failure never causes an already executed command to be re-driven.
func (e *Executor) RunDue(now time.Time) (RunResult, error) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	result := RunResult{Executed: []scheduler.PointingCommand{}, Retry: []scheduler.PointingCommand{}, Retired: []scheduler.PointingCommand{}}
	commands := e.queue.Due(now)
	for index, command := range commands {
		if e.beforeExecute != nil {
			e.beforeExecute(command)
		}
		// A revision replacement may have retired the command after Due handed
		// it out; stale commands must never reach the hardware.
		if !e.queue.IsExecutable(command) {
			if current, found := e.queue.Get(command.ID); found && current.Executed() {
				continue
			}
			result.Retired = append(result.Retired, command)
			continue
		}
		if err := e.Execute(command); err != nil {
			if errors.Is(err, ErrCommandRetired) {
				result.Retired = append(result.Retired, command)
				continue
			}
			result.Failure = &CommandFailure{Command: command, Error: err.Error()}
			e.classifyUnexecuted(commands[index+1:], &result)
			_, _ = e.audit.Record(audit.Event{Component: "antenna", Action: "pointing.failed", Subject: command.AntennaID, Severity: audit.Warning, Fields: map[string]any{"pass_id": command.PassID, "command_id": command.ID, "revision": command.Revision, "error": err.Error(), "executed": len(result.Executed), "retry": len(result.Retry), "retired": len(result.Retired)}})
			return result, result.err()
		}
		// Confirm before moving on: this is the queue consumption boundary.
		// A crash after Execute but before this point may re-drive the command;
		// a crash after it never does.
		if err := e.queue.MarkExecuted(command.ID, now); err != nil {
			// The hardware was driven but the confirmation was not persisted, so
			// the next run will re-drive this command: surface it for retry too.
			result.Failure = &CommandFailure{Command: command, Error: fmt.Sprintf("command executed but confirmation was not persisted: %v", err)}
			result.Retry = append(result.Retry, command)
			e.classifyUnexecuted(commands[index+1:], &result)
			return result, result.err()
		}
		result.Executed = append(result.Executed, command)
	}
	return result, nil
}

// classifyUnexecuted sorts the commands that followed the failure into the
// retry and retired buckets based on their current queue state.
func (e *Executor) classifyUnexecuted(commands []scheduler.PointingCommand, result *RunResult) {
	for _, command := range commands {
		current, found := e.queue.Get(command.ID)
		switch {
		case found && current.Executed():
			continue
		case !found || current.Retired || current.Revision != command.Revision:
			result.Retired = append(result.Retired, command)
		default:
			result.Retry = append(result.Retry, command)
		}
	}
}

func (e *Executor) Execute(command scheduler.PointingCommand) error {
	if !e.queue.IsExecutable(command) {
		return fmt.Errorf("%w: %s", ErrCommandRetired, command.ID)
	}
	e.fleet.mu.Lock()
	defer e.fleet.mu.Unlock()
	state, found := e.fleet.states[command.AntennaID]
	if !found {
		return fmt.Errorf("antenna %s not found", command.AntennaID)
	}
	if state.OwnerPassID != command.PassID {
		return fmt.Errorf("pointing command owner no longer controls antenna")
	}
	state.Azimuth = command.Azimuth
	state.Elevation = command.Elevation
	state.Mode = Tracking
	state.LastCommandAt = time.Now().UTC()
	if err := state.ValidatePosition(); err != nil {
		return err
	}
	if err := e.fleet.update(state); err != nil {
		return err
	}
	_, _ = e.audit.Record(audit.Event{Component: "antenna", Action: "pointing.executed", Subject: state.ID, Fields: map[string]any{"pass_id": command.PassID, "revision": command.Revision, "azimuth": command.Azimuth, "elevation": command.Elevation}})
	return nil
}
