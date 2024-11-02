package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/getsentry/sentry-go"
	"github.com/go-chi/chi/v5"
	"go.mongodb.org/mongo-driver/mongo"
)

func uploadFile(w http.ResponseWriter, r *http.Request) {
	// Get authed user
	user, err := getUserByToken(r.Header.Get("Authorization"))
	if err != nil {
		sentry.CaptureException(err)
		http.Error(w, "Invalid or missing token", http.StatusUnauthorized)
		return
	}

	// Get file from request body
	file, header, err := r.FormFile("file")
	if err != nil {
		if err != http.ErrMissingFile {
			sentry.CaptureException(err)
		}
		http.Error(w, "Invalid form", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Make sure file doesn't exceeed maximum size
	maxIconSizeMib, _ := strconv.ParseInt(os.Getenv("MAX_ICON_SIZE_MIB"), 10, 32)
	maxEmojiSizeMib, _ := strconv.ParseInt(os.Getenv("MAX_EMOJI_SIZE_MIB"), 10, 32)
	maxStickerSizeMib, _ := strconv.ParseInt(os.Getenv("MAX_STICKER_SIZE_MIB"), 10, 32)
	maxAttachmentSizeMib, _ := strconv.ParseInt(os.Getenv("MAX_ATTACHMENT_SIZE_MIB"), 10, 32)
	if header.Size > map[string]int64{
		"icons":       (maxIconSizeMib << 20),
		"emojis":      (maxEmojiSizeMib << 20),
		"stickers":    (maxStickerSizeMib << 20),
		"attachments": (maxAttachmentSizeMib << 20),
	}[chi.URLParam(r, "bucket")] {
		http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Ingest file
	f, err := IngestMultipartFile(chi.URLParam(r, "bucket"), file, header, user)
	if err != nil {
		if err == ErrUnsupportedFile {
			http.Error(w, "Unsupported file format", http.StatusForbidden)
		} else if err == ErrFileBlocked {
			http.Error(w, "File blocked", http.StatusForbidden)
		} else {
			log.Println(err)
			sentry.CaptureException(err)
			http.Error(w, "Failed to ingest file", http.StatusInternalServerError)
		}
		return
	}

	// Return file details
	encoded, err := json.Marshal(f)
	if err != nil {
		sentry.CaptureException(err)
		http.Error(w, "Failed to send file details", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(encoded)
}

func downloadFile(w http.ResponseWriter, r *http.Request) {
	// Get file
	f, err := GetFile(chi.URLParam(r, "id"))
	if err != nil || f.Bucket != chi.URLParam(r, "bucket") {
		if err != nil && err != mongo.ErrNoDocuments {
			sentry.CaptureException(err)
		}
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	// Caching
	if r.Header.Get("ETag") == f.Id || r.Header.Get("If-None-Match") == f.Id {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Get object
	var thumbnail bool
	if strings.HasPrefix(f.Mime, "image/") && (r.URL.Query().Has("thumbnail") || r.URL.Query().Has("preview")) {
		thumbnail = true
	} else if strings.HasPrefix(f.Mime, "video/") && r.URL.Query().Has("thumbnail") {
		thumbnail = true
	}
	obj, err := f.GetObject(thumbnail)
	if err != nil {
		sentry.CaptureException(err)
		http.Error(w, "Failed to get object", http.StatusInternalServerError)
		return
	}

	// Set response headers
	if thumbnail {
		w.Header().Set("Content-Type", f.ThumbnailMime)
	} else {
		w.Header().Set("Content-Type", f.Mime)
	}
	w.Header().Set("Content-Length", strconv.FormatInt(f.Size, 10))
	w.Header().Set("ETag", f.Id)
	w.Header().Set("Cache-Control", "pbulic, max-age=31536000") // 1 year cache (files should never change)
	filename := chi.URLParam(r, "*")
	if filename == "" {
		filename = f.Id
	}
	isMedia := strings.HasPrefix(f.Mime, "image/") || strings.HasPrefix(f.Mime, "video/") || strings.HasPrefix(f.Mime, "audio/")
	if r.URL.Query().Has("download") || !isMedia {
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%s`, filename))
	} else {
		w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename=%s`, filename))
	}

	// Copy the object data into the response body
	_, err = io.Copy(w, obj)
	if err != nil {
		sentry.CaptureException(err)
		log.Println(err)
		http.Error(w, "Failed to send object", http.StatusInternalServerError)
		return
	}
}
