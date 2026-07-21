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

package s3

import (
	"fmt"
	"log/slog"

	"github.com/aws/smithy-go/logging"
)

// LogWrapper adapts smithy-go's logging.Logger interface onto an *slog.Logger,
// emitting the AWS SDK's request/response diagnostics at debug level.
type LogWrapper struct {
	logger *slog.Logger
}

// Logf implements logging.Logger. The SDK's contract is fmt.Printf-style: it
// passes a format string plus arguments, so we render them and emit the result
// as the slog message itself (at debug level, since the SDK only calls this
// when a debug ClientLogMode is enabled) rather than burying it in positional
// attributes. The logging.Classification is ignored — everything routes to
// debug.
func (lw LogWrapper) Logf(_ logging.Classification, format string, v ...any) {
	lw.logger.Debug(fmt.Sprintf(format, v...))
}
