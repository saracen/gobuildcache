package main

import (
	"log/slog"
	"sync/atomic"
	"time"
)

// Stats counts cache activity for one GOCACHEPROG process. The go command
// starts one per invocation, so these describe a single go build/test/vet.
type Stats struct {
	Gets      atomic.Int64
	Hits      atomic.Int64
	Misses    atomic.Int64
	GetErrors atomic.Int64

	// RemoteLookups counts actions that weren't known locally and were looked
	// up in the bucket; Downloads counts outputs fetched as a result.
	RemoteLookups atomic.Int64
	Downloads     atomic.Int64
	DownloadBytes atomic.Int64

	Puts      atomic.Int64
	PutBytes  atomic.Int64
	PutErrors atomic.Int64

	Uploads        atomic.Int64
	UploadBytes    atomic.Int64
	UploadsSkipped atomic.Int64
	UploadErrors   atomic.Int64

	// Refreshes counts objects refreshed by copying them onto themselves;
	// RefreshUploads those that were uploaded again instead.
	Refreshes      atomic.Int64
	RefreshUploads atomic.Int64
	RefreshErrors  atomic.Int64

	// Retries counts bucket calls tried again after a possibly transient
	// error.
	Retries atomic.Int64

	// RemoteCalls counts calls to the bucket, and RemoteWaitMillis the time
	// spent in them, retries included. Calls overlap, so the wait can exceed
	// how long the process ran; RemotePeakInFlight is how many overlapped at
	// most, and RemoteSlowestMillis the longest single call.
	RemoteCalls         atomic.Int64
	RemoteWaitMillis    atomic.Int64
	RemoteSlowestMillis atomic.Int64
	RemotePeakInFlight  atomic.Int64
	remoteInFlight      atomic.Int64

	// Started is when the process started, for its running time.
	Started time.Time
}

func (s *Stats) Log() {
	var ranMillis int64
	if !s.Started.IsZero() {
		ranMillis = time.Since(s.Started).Milliseconds()
	}

	slog.Info("gobuildcache stats",
		"ran_ms", ranMillis,
		"gets", s.Gets.Load(),
		"hits", s.Hits.Load(),
		"misses", s.Misses.Load(),
		"get_errors", s.GetErrors.Load(),
		"remote_lookups", s.RemoteLookups.Load(),
		"downloads", s.Downloads.Load(),
		"download_bytes", s.DownloadBytes.Load(),
		"puts", s.Puts.Load(),
		"put_bytes", s.PutBytes.Load(),
		"put_errors", s.PutErrors.Load(),
		"uploads", s.Uploads.Load(),
		"upload_bytes", s.UploadBytes.Load(),
		"uploads_skipped", s.UploadsSkipped.Load(),
		"upload_errors", s.UploadErrors.Load(),
		"refreshes", s.Refreshes.Load(),
		"refresh_uploads", s.RefreshUploads.Load(),
		"refresh_errors", s.RefreshErrors.Load(),
		"retries", s.Retries.Load(),
		"remote_calls", s.RemoteCalls.Load(),
		"remote_wait_ms", s.RemoteWaitMillis.Load(),
		"remote_slowest_ms", s.RemoteSlowestMillis.Load(),
		"remote_peak_in_flight", s.RemotePeakInFlight.Load(),
	)
}
