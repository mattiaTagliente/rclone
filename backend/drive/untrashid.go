package drive

// The "untrashid" backend command restores a trashed object by its ID
// to a path, refusing a path that already holds an object.

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/rclone/rclone/fs"
	drive "google.golang.org/api/drive/v3"
)

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
// dest's folder (made when missing), keeping its modification time.
//
// A dest that holds an object when it is checked is refused, so a
// restore never replaces an object. Drive allows two objects of the same
// name in a folder and has no conditional update, so the check is not a
// lock: an object a concurrent writer creates at dest between the check
// and the update leaves two objects of that name, as two concurrent
// uploads of one name would.
func (f *Fs) untrashID(ctx context.Context, id, dest string) (untrashResult, error) {
	var res untrashResult
	dir, leaf := path.Split(dest)
	if leaf == "" || leaf == "." || leaf == ".." {
		return res, fmt.Errorf("%q names no file: give the path of the object to restore", dest)
	}
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
		info, err = f.svc.Files.Get(actualID(id)).Fields("id,name,parents,trashed,mimeType,modifiedTime").
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
	dirID, err := f.dirCache.FindDir(ctx, path.Clean("/" + dir)[1:], true)
	if err != nil {
		return res, fmt.Errorf("couldn't find or make the folder of %q: %w", dest, err)
	}
	// Drive stamps the update time as modifiedTime when the update
	// renames or moves the object, so the restore sends the object's own
	// modifiedTime back.
	update := &drive.File{
		Name:            f.opt.Enc.FromStandardName(leaf),
		Trashed:         false,
		ModifiedTime:    info.ModifiedTime,
		ForceSendFields: []string{"Trashed"}, // necessary to send the false value
	}
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
