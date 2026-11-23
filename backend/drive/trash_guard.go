package drive

// A write that resolved its folder from the directory cache can land inside
// a folder another process trashed in the seconds before this process's
// change poll reported it: the upload succeeds and the file is lost in the
// trash. The change poll flushes such a folder (flushGoneDir), but only
// after the poll interval plus the lag of Drive's change feed (measured
// 1.7 to 5.8 s on top of the interval).
//
// So after every write that places an object under a cached folder, one
// query asks whether a folder on that path is trashed. Drive flags the
// folder that was trashed at once, while the folders below it are flagged
// over the following seconds, so the query looks for every folder of the
// path by (parent, name) and keeps only the IDs the cache holds. A hit
// flushes the cache from the shallowest trashed folder, resolves the path
// anew (creating the folders outside the trash) and moves the object there.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/dircache"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

// pathFolder is one folder of a path: its cached ID, its parent's ID, its
// name on Drive and its path relative to the root.
type pathFolder struct {
	path     string
	name     string
	id       string
	parentID string
}

// cachedFolders returns the folders of dir (relative to the root, root
// excluded) the directory cache holds, from the top down, stopping at the
// first one it does not hold.
func cachedFolders(dc *dircache.DirCache, rootID, dir string, fromStandardName func(string) string) []pathFolder {
	if dir == "" {
		return nil
	}
	var out []pathFolder
	parentID := rootID
	prefix := ""
	for _, name := range strings.Split(dir, "/") {
		if prefix == "" {
			prefix = name
		} else {
			prefix += "/" + name
		}
		id, ok := dc.Get(prefix)
		if !ok {
			break
		}
		id = actualID(id)
		out = append(out, pathFolder{path: prefix, name: fromStandardName(name), id: id, parentID: parentID})
		parentID = id
	}
	return out
}

// escapeQueryName quotes a Drive name for a query string
func escapeQueryName(name string) string {
	name = strings.ReplaceAll(name, `\`, `\\`)
	return strings.ReplaceAll(name, `'`, `\'`)
}

// trashedFolderQuery is the query that lists the trashed folders among
// folders, each found by its parent and name.
func trashedFolderQuery(folders []pathFolder) string {
	terms := make([]string, 0, len(folders))
	for _, d := range folders {
		terms = append(terms, fmt.Sprintf("(name='%s' and '%s' in parents)", escapeQueryName(d.name), d.parentID))
	}
	return fmt.Sprintf("trashed=true and mimeType='%s' and (%s)", driveFolderType, strings.Join(terms, " or "))
}

// shallowestTrashed returns the index in folders of the first folder whose
// ID is in trashed, or -1. A trashed folder with the same parent and name
// but another ID (an earlier delete of the same path) is not on the path.
func shallowestTrashed(folders []pathFolder, trashed map[string]bool) int {
	for i, d := range folders {
		if trashed[d.id] {
			return i
		}
	}
	return -1
}

// trashedOnPath returns the path of the shallowest cached folder of dir
// that is trashed, or "" when none is.
func (f *Fs) trashedOnPath(ctx context.Context, dir string) (string, error) {
	rootID, err := f.dirCache.RootID(ctx, false)
	if err != nil {
		return "", err
	}
	folders := cachedFolders(f.dirCache, actualID(rootID), dir, f.opt.Enc.FromStandardName)
	if len(folders) == 0 {
		return "", nil
	}
	trashed := map[string]bool{}
	pageToken := ""
	for {
		var files *drive.FileList
		err = f.pacer.Call(func() (bool, error) {
			list := f.svc.Files.List().Q(trashedFolderQuery(folders)).
				Fields("nextPageToken,files(id)").
				PageSize(1000).
				SupportsAllDrives(true).
				IncludeItemsFromAllDrives(true)
			if f.isTeamDrive && !f.opt.SharedWithMe {
				list.DriveId(f.opt.TeamDriveID)
				list.Corpora("drive")
			}
			if pageToken != "" {
				list.PageToken(pageToken)
			}
			files, err = list.Context(ctx).Do()
			return f.shouldRetry(ctx, err)
		})
		if err != nil {
			return "", fmt.Errorf("checking %q for a trashed folder: %w", dir, err)
		}
		for _, item := range files.Files {
			trashed[item.Id] = true
		}
		if files.NextPageToken == "" {
			break
		}
		pageToken = files.NextPageToken
	}
	if i := shallowestTrashed(folders, trashed); i >= 0 {
		return folders[i].path, nil
	}
	return "", nil
}

// keepOutOfTrash is called after a write placed the object id (a file, or
// a folder when isDir) at remote inside the folder parentID. When a folder
// of that path is trashed it moves the object to the path resolved anew
// and returns the moved object's info; else it returns nil.
func (f *Fs) keepOutOfTrash(ctx context.Context, remote, id, parentID string, isDir bool) (*drive.File, error) {
	if f.opt.TrashedOnly {
		return nil, nil
	}
	dir, _ := dircache.SplitPath(remote)
	gone, err := f.trashedOnPath(ctx, dir)
	if err != nil || gone == "" {
		return nil, err
	}
	fs.Logf(remote, "folder %q was trashed by another process before this write reached it: moving the %s out of the trash", gone, map[bool]string{false: "file", true: "folder"}[isDir])
	f.dirCache.FlushDir(gone)
	_, newParentID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, fmt.Errorf("re-creating the folders of %q outside the trash: %w", remote, err)
	}
	newParentID = actualID(newParentID)
	var info *drive.File
	err = f.pacer.Call(func() (bool, error) {
		info, err = f.svc.Files.Update(actualID(id), &drive.File{Trashed: false, ForceSendFields: []string{"Trashed"}}).
			RemoveParents(actualID(parentID)).
			AddParents(newParentID).
			Fields(f.getFileFields(ctx)).
			SupportsAllDrives(true).
			Context(ctx).Do()
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, fmt.Errorf("moving %q out of the trash: %w", remote, err)
	}
	if isDir {
		f.dirCache.Put(remote, id)
	}
	return info, nil
}

// fileOutOfTrash is keepOutOfTrash for the info a file write returned: the
// moved file's info, or info itself when nothing moved. A file with no
// single parent is not checked.
func (f *Fs) fileOutOfTrash(ctx context.Context, remote string, info *drive.File) (*drive.File, error) {
	if info == nil || len(info.Parents) != 1 {
		return info, nil
	}
	moved, err := f.keepOutOfTrash(ctx, remote, info.Id, info.Parents[0], false)
	if err != nil {
		return nil, err
	}
	if moved != nil {
		return moved, nil
	}
	return info, nil
}

// mkdirOutOfTrash is called after Mkdir resolved dir to a cached or new
// folder. When dir or a folder above it is trashed it flushes the cache from
// there and makes dir anew outside the trash: a mkdir after another
// process's delete gives an empty folder, as on a local filesystem (what the
// trashed folder held was deleted with it).
func (f *Fs) mkdirOutOfTrash(ctx context.Context, dir string) (bool, error) {
	if f.opt.TrashedOnly || dir == "" {
		return false, nil
	}
	gone, err := f.trashedOnPath(ctx, dir)
	if err != nil || gone == "" {
		return false, err
	}
	fs.Logf(dir, "folder %q was trashed by another process: making the folder anew outside the trash", gone)
	f.dirCache.FlushDir(gone)
	_, err = f.dirCache.FindDir(ctx, dir, true)
	return true, err
}

// flushGoneParent drops the cached folders of remote's path when a write
// failed because a folder of it no longer exists (removed for good by
// another process), so the retry resolves the path anew instead of failing
// with 404 until the change poll reports the removal.
func (f *Fs) flushGoneParent(remote string, err error) {
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != 404 {
		return
	}
	dir, _ := dircache.SplitPath(remote)
	if dir == "" {
		return // the root: never flushed here
	}
	top, _, _ := strings.Cut(dir, "/")
	fs.Debugf(remote, "write failed with 404: flushing the directory cache of %q", top)
	f.dirCache.FlushDir(top)
}
