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
	ThumbnailMime string `bson:"thumbnail_mime,omitempty" json:"thumbnail_mime,omitempty"`
	Size          int64  `bson:"size" json:"size"`
	Filename      string `bson:"filename,omitempty" json:"filename,omitempty"`
	Width         int    `bson:"width,omitempty" json:"width,omitempty"`
	Height        int    `bson:"height,omitempty" json:"height,omitempty"`

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
	var id, hashHex string
	var info minio.UploadInfo

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
		// Create file details
		f = File{
			Id:           id,
			Hash:         hashHex,
			Bucket:       bucket,
			Filename:     cleanFilename(fileHeader.Filename),
			UploadRegion: s3RegionOrder[0],
			UploadedBy:   uploader.Username,
			UploadedAt:   time.Now().Unix(),
		}

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
		f.Mime = strings.Fields(string(out))[1]

		// Get dimensions and number of frames, if it is an image
		if strings.HasPrefix(f.Mime, "image/") {
			out, err = exec.Command(
				"magick",
				"identify",
				"-format",
				"%w,%h",
				fmt.Sprint(ingestDir, "/original"),
			).Output()
			if err != nil {
				sentry.CaptureException(err)
				return nil, err
			}
			outSlice := strings.Split(string(out), ",")
			f.Width, _ = strconv.Atoi(outSlice[0])
			f.Height, _ = strconv.Atoi(outSlice[1])
		}

		if bucket == "icons" || bucket == "emojis" || bucket == "stickers" {
			// Make sure the file is an image
			if !strings.HasPrefix(f.Mime, "image/") {
				return nil, ErrUnsupportedFile
			}

			// Get frames
			out, err = exec.Command(
				"magick",
				"identify",
				"-format",
				"%n",
				fmt.Sprint(ingestDir, "/original"),
			).Output()
			if err != nil {
				sentry.CaptureException(err)
				return nil, err
			}
			frames := string(out)

			// Choose format to convert to and update mime
			format := "webp"
			f.Mime = "image/webp"
			if frames != "1" {
				format = "gif"
				f.Mime = "image/gif"
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
			if f.Width < desiredSize {
				desiredSize = f.Width
			} else if f.Height < desiredSize {
				desiredSize = f.Height
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
				f.Size = info.Size
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
				f.Width, _ = strconv.Atoi(outSlice[0])
				f.Height, _ = strconv.Atoi(outSlice[1])
			}()

			wg.Wait()
		} else if bucket == "attachments" {
			if strings.HasPrefix(f.Mime, "image") { // Images
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
							ContentType: f.Mime,
						},
					)
					f.Size = info.Size
				}()

				// Generate thumbnail
				wg.Add(1)
				go func() {
					defer wg.Done()
					err = f.GenerateThumbnail()
				}()

				wg.Wait()

				if err != nil {
					sentry.CaptureException(err)
					return nil, err
				}
			} else if strings.HasPrefix(f.Mime, "video") { // Videos
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
							ContentType: f.Mime,
						},
					)
					f.Size = info.Size
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
				f.Width, _ = strconv.Atoi(outSlice[0])
				f.Height, _ = strconv.Atoi(outSlice[1])

				// Generate thumbnail
				wg.Add(1)
				go func() {
					defer wg.Done()
					err = f.GenerateThumbnail()
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
						ContentType: f.Mime,
					},
				)
				if err != nil {
					sentry.CaptureException(err)
					return nil, err
				}
				f.Size = info.Size
			}
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

func (f *File) GenerateThumbnail() error {
	// Create directory in ingest directory for temporary files
	// And download file for processing
	ingestDir := fmt.Sprint(os.Getenv("INGEST_DIR"), "/", f.Id)
	if _, err := os.Stat(ingestDir); os.IsNotExist(err) {
		defer os.RemoveAll(ingestDir)
		if err := os.Mkdir(ingestDir, 0700); err != nil {
			sentry.CaptureException(err)
			return err
		}

		obj, err := f.GetObject(false)
		if err != nil {
			sentry.CaptureException(err)
			return err
		}

		dst, err := os.Create(fmt.Sprint(ingestDir, "/original"))
		if err != nil {
			sentry.CaptureException(err)
			return err
		}
		defer dst.Close()
		if _, err := io.Copy(dst, obj); err != nil {
			sentry.CaptureException(err)
			return err
		}
	}

	// Choose format to use for the thumbnail
	format := "webp"
	if strings.HasPrefix(f.Mime, "image/") { // use GIF for animated images
		out, err := exec.Command(
			"magick",
			"identify",
			"-format",
			"%n",
			fmt.Sprint(ingestDir, "/original"),
		).Output()
		if err != nil {
			sentry.CaptureException(err)
			return err
		}
		frames := string(out)
		if frames != "1" {
			format = "gif"
		}
	}

	// Use largest axis that is smaller than 480px
	var desiredSize int
	if f.Width > f.Height {
		desiredSize = f.Width
	} else {
		desiredSize = f.Height
	}
	if desiredSize > 480 {
		desiredSize = 480
	}

	// Get first frame if it's a video
	if strings.HasPrefix(f.Mime, "video/") {
		if _, err := os.Stat(fmt.Sprint(ingestDir, "/first_frame.jpg")); os.IsNotExist(err) {
			if err := exec.Command(
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
			).Run(); err != nil {
				sentry.CaptureException(err)
				return err
			}
		}
	}

	// Create thumbnail
	fp := fmt.Sprint(ingestDir, "/original")
	if strings.HasPrefix(f.Mime, "video/") {
		fp = fmt.Sprint(ingestDir, "/first_frame.jpg")
	}
	if err := exec.Command(
		"magick",
		fp,
		"-resize",
		fmt.Sprint(desiredSize, "x", desiredSize),
		fmt.Sprint(ingestDir, "/thumbnail.", format),
	).Run(); err != nil {
		sentry.CaptureException(err)
		return err
	}

	// Upload thumbnail
	if _, err := s3Clients[s3RegionOrder[0]].FPutObject(
		ctx,
		f.Bucket,
		fmt.Sprint(f.Hash, "_thumbnail"),
		fmt.Sprint(ingestDir, "/thumbnail.", format),
		minio.PutObjectOptions{
			ContentType: fmt.Sprint("image/", format),
		},
	); err != nil {
		sentry.CaptureException(err)
		return err
	}

	// Update file details
	f.ThumbnailMime = fmt.Sprint("image/", format)
	if _, err := db.Collection("files").UpdateMany(
		context.TODO(),
		bson.M{"hash": f.Hash, "bucket": f.Bucket},
		bson.M{"$set": bson.M{"thumbnail_mime": f.ThumbnailMime}},
	); err != nil {
		sentry.CaptureException(err)
		return err
	}

	return nil
}

func (f *File) GetObject(thumbnail bool) (*minio.Object, error) {
	objName := f.Hash
	if thumbnail && f.Bucket == "attachments" && (strings.HasPrefix(f.Mime, "image/") || strings.HasPrefix(f.Mime, "video/")) {
		// Generate thumbnail if one doesn't exist yet
		if f.ThumbnailMime == "" {
			if err := f.GenerateThumbnail(); err != nil {
				return nil, err
			}
		}

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
