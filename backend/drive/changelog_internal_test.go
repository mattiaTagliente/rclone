package drive

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	drive "google.golang.org/api/drive/v3"
)

func TestInternalChangeLogSince(t *testing.T) {
	l := newChangeLog(3)
	res := l.since(0, 0)
	assert.True(t, res.Running)
	assert.Equal(t, int64(0), res.Last)
	assert.False(t, res.Overflow)
	assert.Empty(t, res.Changes)

	for i := 1; i <= 2; i++ {
		l.add(changeRecord{ID: fmt.Sprintf("id%d", i)})
	}
	res = l.since(0, 0)
	require.Len(t, res.Changes, 2)
	assert.Equal(t, int64(1), res.Changes[0].Seq)
	assert.Equal(t, "id2", res.Changes[1].ID)
	assert.Equal(t, int64(2), res.Last)

	res = l.since(2, 0)
	assert.Empty(t, res.Changes)
	assert.False(t, res.Overflow)
	assert.Equal(t, int64(2), res.Last)

	// the ring wraps: seq 1 and 2 are dropped once 5 records came
	for i := 3; i <= 5; i++ {
		l.add(changeRecord{ID: fmt.Sprintf("id%d", i)})
	}
	res = l.since(2, 0)
	assert.Equal(t, int64(3), res.First)
	require.Len(t, res.Changes, 3)
	assert.Equal(t, []string{"id3", "id4", "id5"}, []string{res.Changes[0].ID, res.Changes[1].ID, res.Changes[2].ID})
	res = l.since(1, 0)
	assert.True(t, res.Overflow, "a reader at seq 1 missed seq 2")
	assert.Empty(t, res.Changes)

	// max bounds the answer and Last names the last record answered
	res = l.since(2, 2)
	require.Len(t, res.Changes, 2)
	assert.Equal(t, int64(4), res.Last)
}

func TestInternalChangeLogCommand(t *testing.T) {
	f := &Fs{name: "changelogtest", root: "pool"}
	out, err := f.changesCommand(map[string]string{})
	require.NoError(t, err)
	assert.False(t, out.(changesResult).Running, "no change poll: nothing runs")

	f.startChangeLog()
	f.recordChange(&drive.Change{FileId: "f1", File: &drive.File{Name: "a.txt", Trashed: true, MimeType: "text/plain"}},
		[]string{"dir/a.txt"})
	f.recordChange(&drive.Change{FileId: "f2", Removed: true}, nil)
	f.recordChange(&drive.Change{FileId: "d1", File: &drive.File{Name: "dir", MimeType: driveFolderType}}, []string{"dir"})

	out, err = f.changesCommand(map[string]string{"since": "1"})
	require.NoError(t, err)
	res := out.(changesResult)
	require.Len(t, res.Changes, 2)
	assert.Equal(t, "f2", res.Changes[0].ID)
	assert.True(t, res.Changes[0].Removed)
	assert.True(t, res.Changes[1].Folder)
	assert.Equal(t, []string{"dir"}, res.Changes[1].Paths)

	// another instance of the same remote finds the log of the one whose poll runs
	g := &Fs{name: "changelogtest", root: "pool"}
	out, err = g.changesCommand(map[string]string{})
	require.NoError(t, err)
	res = out.(changesResult)
	require.Len(t, res.Changes, 3)
	assert.True(t, res.Changes[0].Trashed)
	assert.Equal(t, []string{"dir/a.txt"}, res.Changes[0].Paths)

	// a position from another epoch is an overflow: the reader lists anew
	out, err = f.changesCommand(map[string]string{"since": "1", "epoch": "another"})
	require.NoError(t, err)
	res = out.(changesResult)
	assert.True(t, res.Overflow)
	assert.Empty(t, res.Changes)

	_, err = f.changesCommand(map[string]string{"since": "x"})
	assert.Error(t, err)
}
