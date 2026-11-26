package drive

import (
	"fmt"
	"math"
	"strconv"
	"sync"
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

	// a position past the last record is not this log's: overflow, and
	// the largest one does not wrap around
	for _, seq := range []int64{6, 1000, math.MaxInt64} {
		res = l.since(seq, 0)
		assert.True(t, res.Overflow, "since=%d", seq)
		assert.Empty(t, res.Changes, "since=%d", seq)
	}
	res = l.since(5, 0)
	assert.False(t, res.Overflow)
	assert.Empty(t, res.Changes)
}

func TestInternalChangeLogCommand(t *testing.T) {
	f := &Fs{name: "changelogtest", root: "pool"}
	out, err := f.changesCommand(map[string]string{})
	require.NoError(t, err)
	assert.False(t, out.(changesResult).Running, "no change poll: nothing runs")

	l := f.startChangeLog()
	defer f.stopChangeLog(l)
	l.record(&drive.Change{FileId: "f1", File: &drive.File{Name: "a.txt", Trashed: true, MimeType: "text/plain"}},
		[]string{"dir/a.txt"})
	l.record(&drive.Change{FileId: "f2", Removed: true}, nil)
	l.record(&drive.Change{FileId: "d1", File: &drive.File{Name: "dir", MimeType: driveFolderType}}, []string{"dir"})

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

func TestInternalChangeLogStop(t *testing.T) {
	f := &Fs{name: "changelogstop", root: "pool"}
	running := func() bool {
		out, err := f.changesCommand(map[string]string{})
		require.NoError(t, err)
		return out.(changesResult).Running
	}

	// a poll that stops withdraws its log
	l1 := f.startChangeLog()
	assert.True(t, running())
	f.stopChangeLog(l1)
	assert.False(t, running(), "a stopped poll's log is not served")

	// a poll that restarts serves a new epoch, and the old log's late
	// stop leaves the new one in place
	l1 = f.startChangeLog()
	l2 := f.startChangeLog()
	f.stopChangeLog(l1)
	out, err := f.changesCommand(map[string]string{})
	require.NoError(t, err)
	res := out.(changesResult)
	assert.True(t, res.Running)
	assert.Equal(t, l2.epoch, res.Epoch)
	assert.NotEqual(t, l1.epoch, l2.epoch)
	f.stopChangeLog(l2)
	assert.False(t, running())

	// two polls of the same remote: stopping the newer one serves the
	// older one, still running, again
	l1 = f.startChangeLog()
	l2 = f.startChangeLog()
	f.stopChangeLog(l2)
	out, err = f.changesCommand(map[string]string{})
	require.NoError(t, err)
	res = out.(changesResult)
	assert.True(t, res.Running, "the older poll still runs")
	assert.Equal(t, l1.epoch, res.Epoch)
	f.stopChangeLog(l1)
	assert.False(t, running())

	// a run that lost the start page token withdraws the log at once: a
	// read before the next token is fetched does not see it running
	l1 = f.startChangeLog()
	assert.Same(t, l1, f.keepChangeLog("token", l1), "a run that kept its token keeps its log")
	assert.True(t, running())
	assert.Nil(t, f.keepChangeLog("", l1))
	assert.False(t, running(), "a log whose token was lost is not served")

	// a nil log (no start page token yet) records and stops as a no-op
	var none *changeLog
	none.record(&drive.Change{FileId: "x"}, nil)
	f.stopChangeLog(none)
}

// Polls record and restart while readers read: run with -race.
func TestInternalChangeLogConcurrent(t *testing.T) {
	f := &Fs{name: "changelogconcurrent", root: "pool"}
	var wg sync.WaitGroup
	for p := 0; p < 2; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				l := f.startChangeLog()
				for i := 0; i < 50; i++ {
					l.record(&drive.Change{FileId: strconv.Itoa(i)}, []string{"p"})
				}
				f.stopChangeLog(l)
			}
		}()
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				out, err := f.changesCommand(map[string]string{"since": strconv.Itoa(i % 60)})
				assert.NoError(t, err)
				res := out.(changesResult)
				for _, c := range res.Changes {
					assert.Greater(t, c.Seq, int64(0))
				}
			}
		}()
	}
	wg.Wait()
	out, err := f.changesCommand(map[string]string{})
	require.NoError(t, err)
	assert.False(t, out.(changesResult).Running, "every poll stopped")
}
