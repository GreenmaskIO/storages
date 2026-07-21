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

package azure

import (
	"context"
	"log/slog"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
)

// setupLogging routes the Azure SDK's request/response logging into the given
// slog logger at debug level, mirroring the s3 storage LogWrapper so the
// underlying API calls can be explored when troubleshooting. It is a no-op
// unless the logger is enabled at debug level.
//
// NOTE: unlike the aws-sdk logger (set per config), the azcore log listener is
// process-wide (a package-level singleton). Consumers using a single storage per
// run are unaffected by the global scope; with multiple concurrent Azure
// storages the last configured listener wins. Auth headers and query parameters
// (e.g. SAS tokens) are redacted by the SDK before reaching the listener.
func setupLogging(ctx context.Context, logger *slog.Logger) {
	if !logger.Enabled(ctx, slog.LevelDebug) {
		return
	}
	azlog.SetEvents(
		azlog.EventRequest,
		azlog.EventResponse,
		azlog.EventResponseError,
		azlog.EventRetryPolicy,
		azlog.EventLRO,
	)
	azlog.SetListener(func(event azlog.Event, msg string) {
		logger.Debug(msg, "class", string(event))
	})
}
