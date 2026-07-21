package storages

import (
	"errors"
	"fmt"
)

// ErrFileNotFound reports that a storage operation was asked to act on
// something that is not an existing object. Test for it with errors.Is rather
// than comparing error strings.
//
// It is returned by GetObject, and wrapped by the *MissingObjectsError that
// Delete and DeleteAll return. Exists and Stat do not use it: they report
// absence through their own return values (a false, or ObjectStat.Exist), so a
// non-nil error from those always means the lookup itself failed.
var ErrFileNotFound = errors.New("file not found")

// MissingObjectsError is returned by Delete and DeleteAll when some of the
// requested paths are not existing objects — either absent, or (for Delete) a
// directory rather than an object. Nothing was deleted: both operations verify
// every path before removing anything.
//
// It wraps ErrFileNotFound, so callers that only care whether something was
// missing can use errors.Is; callers that need to report which paths can reach
// them with errors.As:
//
//	var missing *storages.MissingObjectsError
//	if errors.As(err, &missing) {
//		log.Printf("not found: %v", missing.Paths)
//	}
type MissingObjectsError struct {
	// Paths holds the requested paths that were not existing objects, in the
	// order they were passed. They are relative to the storage's cwd, matching
	// what the caller supplied.
	Paths []string
}

func (e *MissingObjectsError) Error() string {
	if len(e.Paths) == 1 {
		return fmt.Sprintf("object %q not found", e.Paths[0])
	}
	return fmt.Sprintf("objects not found: %q", e.Paths)
}

func (e *MissingObjectsError) Unwrap() error { return ErrFileNotFound }
