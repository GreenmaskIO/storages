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

// The age payload: a 16-byte nonce, then 64 KiB chunks of ChaCha20-Poly1305,
// each carrying a 16-byte tag; an empty plaintext is one empty chunk.
const (
	chunkSize    = 64 * 1024
	tagSize      = 16
	encChunkSize = chunkSize + tagSize
)

// layout maps plaintext offsets and sizes onto an object encrypted to one
// recipient, whose header has a fixed length.
type layout struct {
	// payloadOffset is the header's length plus the nonce: where chunk 0 starts.
	payloadOffset int64
}

// plaintextSize returns the plaintext size of an object of encrypted bytes, and
// false for a size no age file to this recipient can have.
func (l layout) plaintextSize(encrypted int64) (int64, bool) {
	payload := encrypted - l.payloadOffset
	if payload < tagSize {
		return 0, false
	}
	chunks := (payload + encChunkSize - 1) / encChunkSize
	plain := payload - chunks*tagSize
	want := max((plain+chunkSize-1)/chunkSize, 1)
	if want != chunks {
		return 0, false
	}
	return plain, true
}

// reportedSize is what Stat and List say of an object: its plaintext size, or
// the stored size as is for one the formula does not fit.
func (l layout) reportedSize(encrypted int64) int64 {
	if plain, ok := l.plaintextSize(encrypted); ok {
		return plain
	}
	return encrypted
}

// chunkSpan returns the encrypted byte range holding plaintext [off, end).
func (l layout) chunkSpan(off, end, encrypted int64) (start, stop int64) {
	first := off / chunkSize
	last := (end - 1) / chunkSize
	return l.payloadOffset + first*encChunkSize, min(l.payloadOffset+(last+1)*encChunkSize, encrypted)
}

// finalChunkOffset returns where the last chunk of an object of encrypted bytes
// starts; age reads that chunk whenever it opens an object for random access.
func (l layout) finalChunkOffset(encrypted int64) int64 {
	payload := encrypted - l.payloadOffset
	chunks := (payload + encChunkSize - 1) / encChunkSize
	return l.payloadOffset + (chunks-1)*encChunkSize
}
