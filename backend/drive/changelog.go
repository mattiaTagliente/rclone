package drive

// The change log: the change events this Fs's change poll received,
// kept in a bounded ring and read back with the "changes" backend
// command, so a process that keeps its own view of the remote (a census
// of a folder) can stay current from the events the mount already
// receives, without a second poller and a second Drive auth.

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	drive "google.golang.org/api/drive/v3"
)

// changeLogSize is the number of change events the ring keeps; a reader
// that falls further behind is told so (Overflow) and lists anew.
const changeLogSize = 20000

// changeRecord is one change event as the change poll received it.
type changeRecord struct {
	Seq     int64    `json:"seq"`
	Time    string   `json:"time"`
	ID      string   `json:"id"`
	Removed bool     `json:"removed,omitempty"`
	Trashed bool     `json:"trashed,omitempty"`
	Folder  bool     `json:"folder,omitempty"`
	Name    string   `json:"name,omitempty"`
	Paths   []string `json:"paths,omitempty"` // the old and new paths the dir cache resolves
}

// changeLog is a bounded ring of change records.
type changeLog struct {
	mu      sync.Mutex
	epoch   string // new for each change poll started: a reader whose epoch differs lists anew
	started time.Time
	next    int64 // the seq the next record gets; the first is 1
	ring    []changeRecord
	size    int
}

func newChangeLog(size int) *changeLog {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return &changeLog{epoch: hex.EncodeToString(b), started: time.Now(), next: 1, size: size}
}

func (l *changeLog) add(r changeRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r.Seq = l.next
	l.next++
	if len(l.ring) < l.size {
		l.ring = append(l.ring, r)
		return
	}
	l.ring[int((r.Seq-1)%int64(l.size))] = r
}

// changesResult is what the "changes" command answers.
type changesResult struct {
	Running  bool           `json:"running"`
	Epoch    string         `json:"epoch,omitempty"`
	Started  string         `json:"started,omitempty"`
	Last     int64          `json:"last"`     // the seq of the newest record, 0 when none
	First    int64          `json:"first"`    // the seq of the oldest record kept
	Overflow bool           `json:"overflow"` // records after since were dropped
	Changes  []changeRecord `json:"changes"`
}

// since answers the records after seq, at most max of them.
func (l *changeLog) since(seq int64, max int) changesResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	last := l.next - 1
	first := last - int64(len(l.ring)) + 1
	res := changesResult{Running: true, Epoch: l.epoch, Started: l.started.UTC().Format(time.RFC3339),
		Last: last, First: first, Changes: []changeRecord{}}
	if len(l.ring) == 0 {
		res.First = 0
	}
	if seq < 0 {
		seq = 0
	}
	// A position past the last record, or before the oldest one kept, is
	// not continuous with this log: the reader lists anew. Compared before
	// any increment, so no seq overflows.
	if seq > last || seq < first-1 {
		res.Overflow = true
		return res
	}
	for s := seq + 1; s <= last && (max <= 0 || len(res.Changes) < max); s++ {
		res.Changes = append(res.Changes, l.ring[int((s-1)%int64(l.size))])
	}
	if n := len(res.Changes); n > 0 {
		res.Last = res.Changes[n-1].Seq
	}
	return res
}

// changeLogs holds the logs of the change polls that run, by the config
// string of their Fs: an rc call may reach another instance of the same
// remote than the mount's (the cache pins the mount's, but not forever).
// Two polls of the same remote each keep their log, oldest first; the
// newest is served, and the other one again once the newest stops.
var (
	changeLogsMu sync.Mutex
	changeLogs   = map[string][]*changeLog{}
)

func (f *Fs) changeLogKey() string {
	return fs.ConfigString(f)
}

// startChangeLog makes the log a change poll records into from a new
// start page token on: a new epoch, since changes before that token
// may have been missed.
func (f *Fs) startChangeLog() *changeLog {
	l := newChangeLog(changeLogSize)
	changeLogsMu.Lock()
	key := f.changeLogKey()
	changeLogs[key] = append(changeLogs[key], l)
	changeLogsMu.Unlock()
	return l
}

// stopChangeLog withdraws l once its poll stopped or lost continuity.
// The logs of other polls stay in place.
func (f *Fs) stopChangeLog(l *changeLog) {
	if l == nil {
		return
	}
	changeLogsMu.Lock()
	defer changeLogsMu.Unlock()
	key := f.changeLogKey()
	logs := changeLogs[key]
	for i, x := range logs {
		if x == l {
			logs = append(logs[:i:i], logs[i+1:]...)
			break
		}
	}
	if len(logs) == 0 {
		delete(changeLogs, key)
	} else {
		changeLogs[key] = logs
	}
}

// keepChangeLog returns the log a poll records into after a run that
// left it the start page token: none after a run that lost the token,
// since the next token may skip changes, so the log is withdrawn at once
// rather than served as running until that token is fetched.
func (f *Fs) keepChangeLog(startPageToken string, l *changeLog) *changeLog {
	if startPageToken == "" {
		f.stopChangeLog(l)
		return nil
	}
	return l
}

func (f *Fs) findChangeLog() *changeLog {
	changeLogsMu.Lock()
	defer changeLogsMu.Unlock()
	logs := changeLogs[f.changeLogKey()]
	if len(logs) == 0 {
		return nil
	}
	return logs[len(logs)-1]
}

// record keeps one change event with the paths the dir cache resolved
// for it. A nil log records nothing.
func (l *changeLog) record(change *drive.Change, paths []string) {
	if l == nil || change == nil {
		return
	}
	r := changeRecord{Time: time.Now().UTC().Format(time.RFC3339Nano), ID: change.FileId,
		Removed: change.Removed, Paths: paths}
	if change.File != nil {
		r.Trashed = change.File.Trashed
		r.Folder = change.File.MimeType == driveFolderType
		r.Name = change.File.Name
	}
	l.add(r)
}

// changesCommand answers the "changes" backend command.
func (f *Fs) changesCommand(opt map[string]string) (any, error) {
	l := f.findChangeLog()
	if l == nil {
		return changesResult{Running: false, Changes: []changeRecord{}}, nil
	}
	var since int64
	if s, ok := opt["since"]; ok && s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("since: %w", err)
		}
		since = v
	}
	max := 5000
	if s, ok := opt["max"]; ok && s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("max: %w", err)
		}
		max = v
	}
	res := l.since(since, max)
	if e, ok := opt["epoch"]; ok && e != "" && e != res.Epoch {
		// the reader's position belongs to another poll: everything since is unknown
		res.Overflow, res.Changes = true, []changeRecord{}
	}
	return res, nil
}
