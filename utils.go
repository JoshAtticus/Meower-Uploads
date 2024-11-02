package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrUnsupportedFile = errors.New("unsupported file")
	ErrFileBlocked     = errors.New("file blocked")
)

func generateId() (string, error) {
	// Generate bytes
	b := make([]byte, 18)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	// Construct ID
	id := base64.URLEncoding.EncodeToString(b)
	id = strings.ReplaceAll(id, "-", "a")
	id = strings.ReplaceAll(id, "_", "b")
	id = strings.ReplaceAll(id, "=", "c")
	return id, err
}

func cleanFilename(filename string) string {
	re := regexp.MustCompile(`[^A-Za-z0-9\.\-\_\+\!\(\)$]`)
	return re.ReplaceAllString(filename, "_")
}

// Delete unclaimed files that are more than 30 minutes old
func cleanupFiles() error {
	cur, err := db.Collection("files").Find(context.TODO(), bson.M{
		"claimed":     false,
		"uploaded_at": bson.M{"$lt": time.Now().Unix() - 1800},
	})
	if err != nil {
		return err
	}

	var files []File
	if err := cur.All(context.TODO(), &files); err != nil {
		return err
	}

	for _, file := range files {
		if err := file.Delete(); err != nil {
			return err
		}
	}

	return nil
}

func isFileReferenced(bucket string, hashHex string) (bool, error) {
	opts := options.Count()
	opts.SetLimit(1)
	count, err := db.Collection("files").CountDocuments(
		context.TODO(),
		bson.M{"hash": hashHex, "bucket": bucket},
		opts,
	)
	return count > 0, err
}

// Get the block status of a file by its hash.
// Returns whether it's blocked.
func getBlockStatus(hashHex string) (bool, error) {
	opts := options.Count()
	opts.SetLimit(1)
	count, err := db.Collection("blocked_files").CountDocuments(
		context.TODO(),
		bson.M{"_id": hashHex},
		opts,
	)
	return count > 0, err
}
