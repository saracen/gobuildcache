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

	// RemoteDisabled is 1 if the bucket was turned off, by this process's
	// breaker or because another process sharing the local cache had.
	RemoteDisabled atomic.Int64

	// ClaimWaits counts misses that waited for another process computing the
	// same action, ClaimHits those that then got a hit, and ClaimTimeouts
	// those that gave up waiting.
	ClaimWaits      atomic.Int64
	ClaimWaitMillis atomic.Int64
	ClaimHits       atomic.Int64
	ClaimTimeouts   atomic.Int64

	// DeltaHits counts gets answered from the delta dir, DeltaPuts the puts
	// stored there, and DeltaPutBytes the size of the outputs they added.
	DeltaHits     atomic.Int64
	DeltaPuts     atomic.Int64
	DeltaPutBytes atomic.Int64

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
		"remote_disabled", s.RemoteDisabled.Load(),
		"claim_waits", s.ClaimWaits.Load(),
		"claim_wait_ms", s.ClaimWaitMillis.Load(),
		"claim_hits", s.ClaimHits.Load(),
		"claim_timeouts", s.ClaimTimeouts.Load(),
		"delta_hits", s.DeltaHits.Load(),
		"delta_puts", s.DeltaPuts.Load(),
		"delta_put_bytes", s.DeltaPutBytes.Load(),
	)
}

// remoteCall counts a call to the bucket, returning a function to call when
// it has finished, retries included.
func (s *Stats) remoteCall() func() {
	s.RemoteCalls.Add(1)
	inFlight := s.remoteInFlight.Add(1)
	for {
		peak := s.RemotePeakInFlight.Load()
		if inFlight <= peak || s.RemotePeakInFlight.CompareAndSwap(peak, inFlight) {
			break
		}
	}

	start := time.Now()
	return func() {
		s.remoteInFlight.Add(-1)

		took := time.Since(start).Milliseconds()
		s.RemoteWaitMillis.Add(took)
		for {
			slowest := s.RemoteSlowestMillis.Load()
			if took <= slowest || s.RemoteSlowestMillis.CompareAndSwap(slowest, took) {
				break
			}
		}
	}
}
