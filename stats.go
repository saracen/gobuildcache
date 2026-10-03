package main

import (
	"log/slog"
	"sync/atomic"
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
}

func (s *Stats) Log() {
	slog.Info("gobuildcache stats",
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
	)
}
