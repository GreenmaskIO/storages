// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storages

import (
	"context"
	"fmt"
	"path"
)

// Walk recursively lists every file under st, returning slash-separated paths
// relative to st's current working directory. Directories are not included in
// the result — only the files within them.
func Walk(ctx context.Context, st Storager) ([]string, error) {
	return walk(ctx, st, "")
}

// walk carries the accumulated path prefix through the recursion. It is
// unexported so that Walk's own signature does not expose it: every caller
// starts at st's root.
func walk(ctx context.Context, st Storager, parent string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	files, dirs, err := st.ListDir(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing directory: %w", err)
	}

	res := make([]string, 0, len(files))
	for _, f := range files {
		res = append(res, path.Join(parent, f))
	}
	for _, d := range dirs {
		subFiles, err := walk(ctx, d, path.Join(parent, d.Dirname()))
		if err != nil {
			return nil, fmt.Errorf("error walking through directory: %w", err)
		}
		res = append(res, subFiles...)
	}

	return res, nil
}
