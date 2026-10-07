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
	Executed    bool      `json:"executed"`
	ExecutedAt  time.Time `json:"executed_at,omitempty"`
	Retired     bool      `json:"retired"`
	RetireCause string    `json:"retire_cause,omitempty"`
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
		if !command.Retired && !command.Executed && command.Revision == q.active[command.PassID] && !command.ExecuteAt.After(now) {
			values = append(values, command)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ExecuteAt.Before(values[j].ExecuteAt) })
	return values
}

// Acknowledge marks queued commands as executed so Due stops returning them.
// It is idempotent: an already-executed command is reported again without
// changing its ExecutedAt timestamp.
func (q *CommandQueue) Acknowledge(ids ...string) ([]PointingCommand, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	indexes := make([]int, 0, len(ids))
	for _, id := range ids {
		index := -1
		for candidate := range q.commands {
			if q.commands[candidate].ID == id {
				index = candidate
				break
			}
		}
		if index < 0 {
			return nil, fmt.Errorf("pointing command %s is not queued", id)
		}
		indexes = append(indexes, index)
	}
	at := time.Now().UTC()
	acknowledged := make([]PointingCommand, 0, len(ids))
	for _, index := range indexes {
		if !q.commands[index].Executed {
			q.commands[index].Executed = true
			q.commands[index].ExecutedAt = at
		}
		acknowledged = append(acknowledged, q.commands[index])
	}
	if err := store.WriteJSON(q.path, q.commands); err != nil {
		return nil, fmt.Errorf("persist acknowledged pointing commands: %w", err)
	}
	return acknowledged, nil
}

func (q *CommandQueue) IsExecutable(command PointingCommand) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return !command.Retired && !command.Executed && command.Revision == q.active[command.PassID]
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
