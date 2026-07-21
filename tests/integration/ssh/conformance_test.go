// Copyright 2026 Greenmask
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

package ssh

import (
	"testing"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/storagetest"
)

// TestConformance holds this backend to the same Storager contract as every
// other one. Each case gets its own unique prefix in the shared SFTP container.
func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storages.Storager {
		return newTestStorage(t)
	})
}
