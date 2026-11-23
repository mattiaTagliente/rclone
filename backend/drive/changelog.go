package drive

// The change log: the change events this Fs's change poll received,
// kept in a bounded ring and read back with the "changes" backend
// command, so a process that keeps its own view of the remote (a census
// of a folder) can stay current from the events the mount already
// receives, without a second poller and a second Drive auth.
//
// The "untrashid" backend command restores a trashed object by its ID
// to a path, refusing a path that already holds an object.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
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
	if seq+1 < first {
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

// changeLogs holds the log of each Fs whose change poll runs, by its
// config string: an rc call may reach another instance of the same
// remote than the mount's (the cache pins the mount's, but not forever).
var (
	changeLogsMu sync.Mutex
	changeLogs   = map[string]*changeLog{}
)

func (f *Fs) changeLogKey() string {
	return fs.ConfigString(f)
}

// startChangeLog makes the log a new change poll records into.
func (f *Fs) startChangeLog() *changeLog {
	l := newChangeLog(changeLogSize)
	changeLogsMu.Lock()
	changeLogs[f.changeLogKey()] = l
	changeLogsMu.Unlock()
	f.changeLog = l
	return l
}

func (f *Fs) findChangeLog() *changeLog {
	if f.changeLog != nil {
		return f.changeLog
	}
	changeLogsMu.Lock()
	defer changeLogsMu.Unlock()
	return changeLogs[f.changeLogKey()]
}

// recordChange keeps one change event with the paths the dir cache
// resolved for it.
func (f *Fs) recordChange(change *drive.Change, paths []string) {
	l := f.changeLog
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

// untrashResult is what "untrashid" answers per object restored.
type untrashResult struct {
	ID      string `json:"id"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	MD5     string `json:"md5"`
	ModTime string `json:"modTime"`
}

var errTargetExists = errors.New("an object already exists at the target path")

// untrashID restores the trashed object id to dest (relative to the Fs
// root) in one update: out of the trash, under dest's leaf name, in
// dest's folder (made when missing). A dest that already holds an
// object is refused: a restore never overwrites, the caller names
// another path.
func (f *Fs) untrashID(ctx context.Context, id, dest string) (untrashResult, error) {
	var res untrashResult
	if _, err := f.NewObject(ctx, dest); err == nil {
		return res, fmt.Errorf("%q: %w", dest, errTargetExists)
	} else if !errors.Is(err, fs.ErrorObjectNotFound) && !errors.Is(err, fs.ErrorIsDir) {
		return res, err
	} else if errors.Is(err, fs.ErrorIsDir) {
		return res, fmt.Errorf("%q is a directory: %w", dest, errTargetExists)
	}
	var info *drive.File
	err := f.pacer.Call(func() (bool, error) {
		var err error
		info, err = f.svc.Files.Get(actualID(id)).Fields("id,name,parents,trashed,mimeType").
			SupportsAllDrives(true).Context(ctx).Do()
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return res, fmt.Errorf("couldn't read %q: %w", id, err)
	}
	if info.MimeType == driveFolderType {
		return res, fmt.Errorf("%q is a folder: untrash restores objects only", id)
	}
	if !info.Trashed {
		return res, fmt.Errorf("%q is not in the trash", id)
	}
	dir, leaf := path.Split(dest)
	dirID, err := f.dirCache.FindDir(ctx, path.Clean("/" + dir)[1:], true)
	if err != nil {
		return res, fmt.Errorf("couldn't find or make the folder of %q: %w", dest, err)
	}
	update := &drive.File{Name: f.opt.Enc.FromStandardName(leaf)}
	update.Trashed = false
	update.ForceSendFields = []string{"Trashed"}
	call := f.svc.Files.Update(info.Id, update).AddParents(dirID).
		Fields("id,name,parents,size,md5Checksum,modifiedTime,trashed").SupportsAllDrives(true)
	var remove []string
	for _, p := range info.Parents {
		if p != dirID {
			remove = append(remove, p)
		}
	}
	if len(remove) > 0 {
		call.RemoveParents(strings.Join(remove, ","))
	}
	var out *drive.File
	err = f.pacer.Call(func() (bool, error) {
		var err error
		out, err = call.Context(ctx).Do()
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return res, fmt.Errorf("couldn't untrash %q to %q: %w", id, dest, err)
	}
	return untrashResult{ID: out.Id, Path: dest, Size: out.Size, MD5: out.Md5Checksum, ModTime: out.ModifiedTime}, nil
}
