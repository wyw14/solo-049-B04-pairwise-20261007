package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/store"
)

type PointingCommand struct {
	ID          string    `json:"id"`
	PassID      string    `json:"pass_id"`
	AntennaID   string    `json:"antenna_id"`
	Revision    int64     `json:"revision"`
	ExecuteAt   time.Time `json:"execute_at"`
	Azimuth     float64   `json:"azimuth"`
	Elevation   float64   `json:"elevation"`
	ExecutedAt  time.Time `json:"executed_at,omitempty"`
	Retired     bool      `json:"retired"`
	RetireCause string    `json:"retire_cause,omitempty"`
}

// Executed reports whether the command was already driven to the antenna and
// durably confirmed.
func (c PointingCommand) Executed() bool {
	return !c.ExecutedAt.IsZero()
}

type CommandQueue struct {
	mu       sync.RWMutex
	path     string
	commands []PointingCommand
	active   map[string]int64
}

func NewCommandQueue(paths store.Paths) (*CommandQueue, error) {
	queue := &CommandQueue{path: paths.Snapshot("pointing-commands"), active: make(map[string]int64)}
	found, err := store.ReadJSON(queue.path, &queue.commands)
	if err != nil {
		return nil, err
	}
	if found {
		for _, command := range queue.commands {
			if !command.Retired && command.Revision > queue.active[command.PassID] {
				queue.active[command.PassID] = command.Revision
			}
		}
	}
	return queue, nil
}

func (q *CommandQueue) Replace(passID string, revision int64, commands []PointingCommand) error {
	if passID == "" || revision < 1 || len(commands) == 0 {
		return fmt.Errorf("pass, revision and pointing commands are required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for index := range q.commands {
		if q.commands[index].PassID == passID && !q.commands[index].Retired {
			q.commands[index].Retired = true
			q.commands[index].RetireCause = fmt.Sprintf("replaced by revision %d", revision)
		}
	}
	for _, command := range commands {
		command.ID = uuid.NewString()
		command.PassID = passID
		command.Revision = revision
		q.commands = append(q.commands, command)
	}
	q.active[passID] = revision
	return store.WriteJSON(q.path, q.commands)
}

func (q *CommandQueue) Due(now time.Time) []PointingCommand {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var values []PointingCommand
	for _, command := range q.commands {
		if command.Executed() || command.Retired || command.Revision != q.active[command.PassID] || command.ExecuteAt.After(now) {
			continue
		}
		values = append(values, command)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ExecuteAt.Before(values[j].ExecuteAt) })
	return values
}

// MarkExecuted is the queue consumption boundary: it durably records that the
// hardware drive for the command succeeded, so the command is never handed out
// by Due again. Re-confirming an already executed command is a no-op.
func (q *CommandQueue) MarkExecuted(id string, at time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for index := range q.commands {
		if q.commands[index].ID != id {
			continue
		}
		if q.commands[index].Executed() {
			return nil
		}
		q.commands[index].ExecutedAt = at.UTC()
		return store.WriteJSON(q.path, q.commands)
	}
	return fmt.Errorf("pointing command %s not found", id)
}

func (q *CommandQueue) IsExecutable(command PointingCommand) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return !command.Executed() && !command.Retired && command.Revision == q.active[command.PassID]
}

// Get returns the current stored state of a command by ID, reflecting any
// execution confirmation or retirement that happened after it was handed out.
func (q *CommandQueue) Get(id string) (PointingCommand, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, command := range q.commands {
		if command.ID == id {
			return command, true
		}
	}
	return PointingCommand{}, false
}

func (q *CommandQueue) List(passID string) []PointingCommand {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var values []PointingCommand
	for _, command := range q.commands {
		if passID == "" || command.PassID == passID {
			values = append(values, command)
		}
	}
	return values
}
