package main

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/minio/minio-go/v7"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type File struct {
	Id            string `bson:"_id" json:"id"`
	Hash          string `bson:"hash" json:"-"`
	Bucket        string `bson:"bucket" json:"-"`
	Mime          string `bson:"mime" json:"mime"`
	ThumbnailMime string `bson:"thumbnail_mime,omitempty" json:"-"`
	Size          int64  `bson:"size" json:"size"`
	Filename      string `bson:"filename,omitempty" json:"filename"`
	Width         int    `bson:"width,omitempty" json:"width"`
	Height        int    `bson:"height,omitempty" json:"height"`

	UploadRegion string `bson:"upload_region" json:"-"`
	UploadedBy   string `bson:"uploaded_by" json:"-"`
	UploadedAt   int64  `bson:"uploaded_at" json:"-"`

	Claimed bool `bson:"claimed" json:"-"`
}

func IngestMultipartFile(
	bucket string,
	file multipart.File,
	fileHeader *multipart.FileHeader,
	uploader *User,
) (*File, error) {
	// Init vars
	var f File
	var wg sync.WaitGroup
	var err error
	var id, hashHex, mime, thumbnailMime, frames string
	var width, height int

	// Create file ID
	id, err = generateId()
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}

	// Create directory in ingest directory for temporary files
	ingestDir := fmt.Sprint(os.Getenv("INGEST_DIR"), "/", id)
	defer os.RemoveAll(ingestDir)
	if err := os.Mkdir(ingestDir, 0700); err != nil {
		sentry.CaptureException(err)
		return nil, err
	}

	// Save file
	dst, err := os.Create(fmt.Sprint(ingestDir, "/original"))
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, file); err != nil {
		sentry.CaptureException(err)
		return nil, err
	}

	// "Ultra HD"
	if uploader.Flags&FlagUltraHDUploads != 0 {
		out, _ := exec.Command(
			"file",
			"--mime-type",
			fmt.Sprint(ingestDir, "/original"),
		).Output()
		if strings.HasPrefix(strings.Fields(string(out))[1], "image/") {
			exec.Command(
				"magick",
				fmt.Sprint(ingestDir, "/original"),
				"-quality",
				"5",
				fmt.Sprint(ingestDir, "/original.jpg"),
			).Run()
			os.Rename(fmt.Sprint(ingestDir, "/original.jpg"), fmt.Sprint(ingestDir, "/original"))
		}
	}

	// Get file hash
	var out []byte
	out, err = exec.Command(
		"sha256sum",
		fmt.Sprint(ingestDir, "/original"),
	).Output()
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}
	hashHex = strings.Fields(string(out))[0]

	// Make sure file isn't blocked
	if blocked, err := getBlockStatus(hashHex); blocked || err != nil {
		os.Remove(fmt.Sprint(ingestDir, "/original"))
		if blocked {
			return nil, ErrFileBlocked
		}
		return nil, err
	}

	// Attempt to get existing file details
	err = db.Collection("files").FindOne(
		context.TODO(),
		bson.M{"hash": hashHex, "bucket": bucket},
	).Decode(&f)
	if err != nil && err != mongo.ErrNoDocuments {
		return nil, err
	}

	// Process and save file
	if f.Hash == hashHex {
		f.Id = id
		f.Filename = cleanFilename(fileHeader.Filename)
		f.UploadedBy = uploader.Username
		f.UploadedAt = time.Now().Unix()
	} else {
		// Get mime
		out, err = exec.Command(
			"file",
			"--mime-type",
			fmt.Sprint(ingestDir, "/original"),
		).Output()
		if err != nil {
			sentry.CaptureException(err)
			return nil, err
		}
		mime = strings.Fields(string(out))[1]

		// Init MinIO upload info var
		var info minio.UploadInfo

		// Get dimensions and number of frames, if it is an image
		if strings.HasPrefix(mime, "image/") {
			out, err = exec.Command(
				"magick",
				"identify",
				"-format",
				"%w,%h,%n",
				fmt.Sprint(ingestDir, "/original"),
			).Output()
			if err != nil {
				sentry.CaptureException(err)
				return nil, err
			}
			outSlice := strings.Split(string(out), ",")
			width, _ = strconv.Atoi(outSlice[0])
			height, _ = strconv.Atoi(outSlice[1])
			frames = outSlice[2]
		}

		if bucket == "icons" || bucket == "emojis" || bucket == "stickers" {
			// Make sure the file is an image
			if !strings.HasPrefix(mime, "image/") {
				return nil, ErrUnsupportedFile
			}

			// Choose format to convert to and update mime
			format := "webp"
			mime = "image/webp"
			if frames != "1" {
				format = "gif"
				mime = "image/gif"
			}

			// If one of the axis is less than n, use that size rather than n
			var desiredSize int
			switch bucket {
			case "icons":
				desiredSize = 256
			case "emojis":
				desiredSize = 128
			case "stickers":
				desiredSize = 384
			}
			if width < desiredSize {
				desiredSize = width
			} else if height < desiredSize {
				desiredSize = height
			}

			// Remove Exif, optimize, and resize
			err = exec.Command(
				"magick",
				fmt.Sprint(ingestDir, "/original"),
				"-quality",
				"90",
				"-resize",
				fmt.Sprint(desiredSize, "x", desiredSize),
				"+profile",
				"\"*\"",
				fmt.Sprint(ingestDir, "/.", format),
			).Run()
			if err != nil {
				sentry.CaptureException(err)
				return nil, err
			}

			// Upload to bucket
			wg.Add(1)
			go func() {
				defer wg.Done()
				info, err = s3Clients[s3RegionOrder[0]].FPutObject(
					ctx,
					bucket,
					hashHex,
					fmt.Sprint(ingestDir, "/.", format),
					minio.PutObjectOptions{
						ContentType: fmt.Sprint("image/", format),
					},
				)
			}()

			// Get new width and height
			wg.Add(1)
			go func() {
				defer wg.Done()
				out, _ = exec.Command(
					"magick",
					"identify",
					"-format",
					"%w,%h",
					fmt.Sprint(ingestDir, "/.", format),
				).Output()
				outSlice := strings.Split(string(out), ",")
				width, _ = strconv.Atoi(outSlice[0])
				height, _ = strconv.Atoi(outSlice[1])
			}()

			wg.Wait()
		} else if bucket == "attachments" {
			if strings.HasPrefix(mime, "image") { // Images
				// Remove Exif and optimize
				err = exec.Command(
					"magick",
					fmt.Sprint(ingestDir, "/original"),
					"-quality",
					"90",
					"+profile",
					"\"*\"",
					fmt.Sprint(ingestDir, "/optimized"),
				).Run()
				if err != nil {
					sentry.CaptureException(err)
					return nil, err
				}

				// Upload optimized to bucket
				wg.Add(1)
				go func() {
					defer wg.Done()
					info, err = s3Clients[s3RegionOrder[0]].FPutObject(
						ctx,
						bucket,
						hashHex,
						fmt.Sprint(ingestDir, "/optimized"),
						minio.PutObjectOptions{
							ContentType: mime,
						},
					)
				}()

				// Create thumbnail and upload to bucket
				wg.Add(1)
				go func() {
					defer wg.Done()

					// Choose format to use for the thumbnail
					format := "webp"
					thumbnailMime = "image/webp"
					if frames != "1" {
						format = "gif"
						thumbnailMime = "image/gif"
					}

					// Use largest axis that is smaller than 480px
					var desiredSize int
					if width > height {
						desiredSize = width
					} else {
						desiredSize = height
					}
					if desiredSize > 480 {
						desiredSize = 480
					}

					err = exec.Command(
						"magick",
						fmt.Sprint(ingestDir, "/optimized"),
						"-resize",
						fmt.Sprint(desiredSize, "x", desiredSize),
						fmt.Sprint(ingestDir, "/thumbnail.", format),
					).Run()

					_, err = s3Clients[s3RegionOrder[0]].FPutObject(
						ctx,
						bucket,
						fmt.Sprint(hashHex, "_thumbnail"),
						fmt.Sprint(ingestDir, "/thumbnail.", format),
						minio.PutObjectOptions{
							ContentType: fmt.Sprint("image/", format),
						},
					)
				}()

				wg.Wait()

				if err != nil {
					sentry.CaptureException(err)
					return nil, err
				}
			} else if strings.HasPrefix(mime, "video") { // Videos
				// Start uploading video to bucket
				wg.Add(1)
				go func() {
					defer wg.Done()
					info, err = s3Clients[s3RegionOrder[0]].FPutObject(
						ctx,
						bucket,
						hashHex,
						fmt.Sprint(ingestDir, "/original"),
						minio.PutObjectOptions{
							ContentType: mime,
						},
					)
				}()

				// Get first frame
				err = exec.Command(
					"ffmpeg",
					"-i",
					fmt.Sprint(ingestDir, "/original"),
					"-vf",
					"select=eq(n\\,0)",
					"-vsync",
					"vfr",
					"-q:v",
					"2",
					fmt.Sprint(ingestDir, "/first_frame.jpg"),
				).Run()
				if err != nil {
					sentry.CaptureException(err)
					return nil, err
				}

				// Get dimensions from first frame
				out, err = exec.Command(
					"magick",
					"identify",
					"-format",
					"%w,%h",
					fmt.Sprint(ingestDir, "/first_frame.jpg"),
				).Output()
				if err != nil {
					sentry.CaptureException(err)
					return nil, err
				}
				outSlice := strings.Split(string(out), ",")
				width, _ = strconv.Atoi(outSlice[0])
				height, _ = strconv.Atoi(outSlice[1])

				// Create thumbnail and upload to bucket
				wg.Add(1)
				go func() {
					defer wg.Done()

					// Use largest axis that is smaller than 480px
					var desiredSize int
					if width > height {
						desiredSize = width
					} else {
						desiredSize = height
					}
					if desiredSize > 480 {
						desiredSize = 480
					}

					err = exec.Command(
						"magick",
						fmt.Sprint(ingestDir, "/first_frame.jpg"),
						"-resize",
						fmt.Sprint(desiredSize, "x", desiredSize),
						fmt.Sprint(ingestDir, "/thumbnail.webp"),
					).Run()

					_, err = s3Clients[s3RegionOrder[0]].FPutObject(
						ctx,
						bucket,
						fmt.Sprint(hashHex, "_thumbnail"),
						fmt.Sprint(ingestDir, "/thumbnail.webp"),
						minio.PutObjectOptions{
							ContentType: "image/webp",
						},
					)

					thumbnailMime = "image/webp"
				}()

				wg.Wait()

				if err != nil {
					sentry.CaptureException(err)
					return nil, err
				}
			} else { // Everything else
				info, err = s3Clients[s3RegionOrder[0]].FPutObject(
					ctx,
					bucket,
					hashHex,
					fmt.Sprint(ingestDir, "/original"),
					minio.PutObjectOptions{
						ContentType: mime,
					},
				)
				if err != nil {
					sentry.CaptureException(err)
					return nil, err
				}
			}
		}

		// Create file details
		f = File{
			Id:            id,
			Hash:          hashHex,
			Bucket:        bucket,
			Mime:          mime,
			ThumbnailMime: thumbnailMime,
			Size:          info.Size,
			Filename:      cleanFilename(fileHeader.Filename),
			Width:         width,
			Height:        height,
			UploadRegion:  s3RegionOrder[0],
			UploadedBy:    uploader.Username,
			UploadedAt:    time.Now().Unix(),
		}
	}

	// Create database item
	if _, err := db.Collection("files").InsertOne(context.TODO(), &f); err != nil {
		sentry.CaptureException(err)
		return &f, err
	}

	sentry.CaptureMessage(fmt.Sprintf("Uploaded file %s with hash %s to %s region", f.Id, f.Hash, f.UploadRegion))

	return &f, nil
}

func GetFile(id string) (File, error) {
	var f File
	err := db.Collection("files").FindOne(
		context.TODO(),
		bson.M{"_id": id, "uploaded_at": bson.M{"$ne": 0}},
	).Decode(&f)
	return f, err
}

func (f *File) GetObject(thumbnail bool) (*minio.Object, error) {
	objName := f.Hash
	if thumbnail && (strings.HasPrefix(f.Mime, "image/") || strings.HasPrefix(f.Mime, "video/")) {
		objName += "_thumbnail"
	}

	return s3Clients[s3RegionOrder[0]].GetObject(
		ctx,
		f.Bucket,
		objName,
		minio.GetObjectOptions{},
	)
}

func (f *File) Delete() error {
	// Delete database row
	if _, err := db.Collection("files").DeleteOne(
		context.TODO(),
		bson.M{"_id": f.Id},
	); err != nil {
		return err
	}

	// Clean-up objects if nothing else is referencing them
	referenced, err := isFileReferenced(f.Bucket, f.Hash)
	if err != nil {
		return err
	}
	if !referenced {
		for _, s3Client := range s3Clients {
			go s3Client.RemoveObject(ctx, f.Bucket, f.Hash, minio.RemoveObjectOptions{})
			go s3Client.RemoveObject(ctx, f.Bucket, f.Hash+"_thumbnail", minio.RemoveObjectOptions{})
		}
	}

	return nil
}
