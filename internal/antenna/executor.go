package antenna

import (
	"fmt"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/scheduler"
)

type Executor struct {
	fleet *Fleet
	queue *scheduler.CommandQueue
	audit *audit.Trail
}

func NewExecutor(fleet *Fleet, queue *scheduler.CommandQueue, trail *audit.Trail) *Executor {
	return &Executor{fleet: fleet, queue: queue, audit: trail}
}

// RunResult partitions one RunDue pass: every due command lands in exactly
// one bucket. Executed commands ran and were acknowledged to the queue,
// PendingRetry commands remain due (the failed command first, then the
// commands that were never attempted), and Retired commands were superseded
// by a newer revision before they could run.
type RunResult struct {
	Executed     []scheduler.PointingCommand `json:"executed"`
	PendingRetry []scheduler.PointingCommand `json:"pending_retry"`
	Retired      []scheduler.PointingCommand `json:"retired"`
	Failure      string                      `json:"failure,omitempty"`
}

func (e *Executor) RunDue(now time.Time) (RunResult, error) {
	return e.runCommands(e.queue.Due(now))
}

func (e *Executor) runCommands(commands []scheduler.PointingCommand) (RunResult, error) {
	result := RunResult{
		Executed:     make([]scheduler.PointingCommand, 0, len(commands)),
		PendingRetry: []scheduler.PointingCommand{},
		Retired:      []scheduler.PointingCommand{},
	}
	for index, command := range commands {
		if !e.queue.IsExecutable(command) {
			// A replan retired this revision after the due snapshot was taken.
			result.Retired = append(result.Retired, command)
			continue
		}
		if err := e.Execute(command); err != nil {
			result.PendingRetry = append(result.PendingRetry, commands[index:]...)
			result.Failure = err.Error()
			return result, fmt.Errorf("run due pointing commands: %w", err)
		}
		// Acknowledge each command before moving on so a later failure can
		// never cause an already-executed command to run again.
		acknowledged, err := e.queue.Acknowledge(command.ID)
		if err != nil {
			result.PendingRetry = append(result.PendingRetry, commands[index:]...)
			result.Failure = err.Error()
			return result, fmt.Errorf("acknowledge pointing command %s: %w", command.ID, err)
		}
		result.Executed = append(result.Executed, acknowledged...)
	}
	return result, nil
}

func (e *Executor) Execute(command scheduler.PointingCommand) error {
	if !e.queue.IsExecutable(command) {
		return fmt.Errorf("pointing command %s belongs to a retired revision", command.ID)
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
