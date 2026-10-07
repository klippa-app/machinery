package tasks_test

import (
	"strings"
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestNewChain(t *testing.T) {
	t.Parallel()

	task1 := tasks.Signature{
		Name: "foo",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: interface{}(1),
			},
			{
				Type:  "float64",
				Value: interface{}(1),
			},
		},
	}

	task2 := tasks.Signature{
		Name: "bar",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: interface{}(5),
			},
			{
				Type:  "float64",
				Value: interface{}(6),
			},
		},
	}

	task3 := tasks.Signature{
		Name: "qux",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: interface{}(4),
			},
		},
	}

	chain, err := tasks.NewChain(&task1, &task2, &task3)
	if err != nil {
		t.Fatal(err)
	}

	firstTask := chain.Tasks[0]

	assert.Equal(t, "foo", firstTask.Name)
	assert.Equal(t, "bar", firstTask.OnSuccess[0].Name)
	assert.Equal(t, "qux", firstTask.OnSuccess[0].OnSuccess[0].Name)
}

func TestNewGroup(t *testing.T) {
	t.Parallel()

	task1 := &tasks.Signature{Name: "foo"}
	task2 := &tasks.Signature{Name: "bar"}

	group, err := tasks.NewGroup(task1, task2)
	assert.NoError(t, err)

	assert.Len(t, group.Tasks, 2)
	assert.NotEmpty(t, group.GroupUUID)
	assert.Equal(t, group.GroupUUID, task1.GroupUUID)
	assert.Equal(t, group.GroupUUID, task2.GroupUUID)
	assert.Equal(t, 2, task1.GroupTaskCount)
	assert.Equal(t, 2, task2.GroupTaskCount)
	assert.NotEmpty(t, task1.UUID)
	assert.NotEmpty(t, task2.UUID)
}

func TestNewGroupPreservesExistingUUID(t *testing.T) {
	t.Parallel()

	task1 := &tasks.Signature{Name: "foo", UUID: "task_existing"}

	group, err := tasks.NewGroup(task1)
	assert.NoError(t, err)
	assert.Equal(t, "task_existing", group.Tasks[0].UUID)
}

func TestGroupGetUUIDs(t *testing.T) {
	t.Parallel()

	group := &tasks.Group{
		Tasks: []*tasks.Signature{
			{UUID: "task_1"},
			{UUID: "task_2"},
		},
	}

	assert.Equal(t, []string{"task_1", "task_2"}, group.GetUUIDs())
}

func TestNewChord(t *testing.T) {
	t.Parallel()

	task1 := &tasks.Signature{Name: "foo"}
	task2 := &tasks.Signature{Name: "bar"}
	group, err := tasks.NewGroup(task1, task2)
	assert.NoError(t, err)

	callback := &tasks.Signature{Name: "callback"}

	chord, err := tasks.NewChord(group, callback)
	assert.NoError(t, err)

	assert.NotEmpty(t, callback.UUID)
	assert.True(t, strings.HasPrefix(callback.UUID, "chord_"))
	assert.Same(t, callback, chord.Callback)
	assert.Same(t, group, chord.Group)
	for _, signature := range group.Tasks {
		assert.Same(t, callback, signature.ChordCallback)
	}
}

func TestNewChordPreservesExistingCallbackUUID(t *testing.T) {
	t.Parallel()

	group, err := tasks.NewGroup(&tasks.Signature{Name: "foo"})
	assert.NoError(t, err)

	callback := &tasks.Signature{Name: "callback", UUID: "chord_existing"}

	_, err = tasks.NewChord(group, callback)
	assert.NoError(t, err)
	assert.Equal(t, "chord_existing", callback.UUID)
}
