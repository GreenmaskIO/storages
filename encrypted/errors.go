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

package encrypted

import "errors"

// ErrNoKey reports an object encrypted to another identity than the storage's.
var ErrNoKey = errors.New("object is not encrypted to this identity")

// ErrCorruptObject reports an object that is not a whole age file: truncated,
// altered, overwritten mid-read, or never encrypted.
var ErrCorruptObject = errors.New("corrupt encrypted object")
