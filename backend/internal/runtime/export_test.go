package runtime

import "time"

// SetRequestTimeout shortens the Docker API request timeout for a test.
func SetRequestTimeout(d time.Duration) (restore func()) {
	old := requestTimeout
	requestTimeout = d
	return func() { requestTimeout = old }
}

// SetPullRetryAfter changes how long a failed image download is reported, for a test.
func SetPullRetryAfter(d time.Duration) (restore func()) {
	old := pullRetryAfter
	pullRetryAfter = d
	return func() { pullRetryAfter = old }
}

// SetFlinkPoll shortens how often Delete checks the JobManager is gone, for a test.
func SetFlinkPoll(d time.Duration) (restore func()) {
	old := flinkPoll
	flinkPoll = d
	return func() { flinkPoll = old }
}
