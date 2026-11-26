package drive

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInternalUnTrashIDNeedsAFileName(t *testing.T) {
	f := &Fs{name: "untrashidtest", root: "pool"}
	for _, dest := range []string{"", "dir/", "dir/.", "dir/..", "."} {
		_, err := f.untrashID(context.Background(), "someid", dest)
		assert.ErrorContains(t, err, "names no file", "dest=%q", dest)
	}
}
