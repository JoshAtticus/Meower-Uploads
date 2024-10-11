package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/getsentry/sentry-go"
	pb "github.com/meower-media-co/Meower-Uploads/grpc_uploads"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

type grpcUploadsServer struct {
	pb.UnimplementedUploadsServer
}

func (s grpcUploadsServer) ClaimFile(ctx context.Context, req *pb.ClaimFileReq) (*pb.ClaimFileResp, error) {
	// Check token
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("x-token")) == 0 || md.Get("x-token")[0] != os.Getenv("GRPC_UPLOADS_TOKEN") {
		return nil, ErrUnauthorized
	}

	// Get file
	f, err := GetFile(req.Id)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			sentry.CaptureException(err)
		}
		return nil, err
	}

	// Check bucket
	if f.Bucket != req.Bucket {
		return nil, ErrMismatchedBucket
	}

	// Claim file
	err = f.Claim()
	if err != nil {
		if err != ErrFileAlreadyClaimed {
			sentry.CaptureException(err)
		}
		return nil, err
	}

	// Get object info
	_, objInfo, err := f.GetObject()
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}

	// Return file details
	return &pb.ClaimFileResp{
		Id:       f.Id,
		Mime:     f.Mime,
		Filename: f.Filename,
		Size:     objInfo.Size,
		Width:    int32(f.Width),
		Height:   int32(f.Height),
	}, nil
}

func (s grpcUploadsServer) DeleteFile(ctx context.Context, req *pb.DeleteFileReq) (*emptypb.Empty, error) {
	// Check token
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("x-token")) == 0 || md.Get("x-token")[0] != os.Getenv("GRPC_UPLOADS_TOKEN") {
		return nil, ErrUnauthorized
	}

	// Get file
	f, err := GetFile(req.Id)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			sentry.CaptureException(err)
		}
		return nil, err
	}

	// Delete file
	if err := f.Delete(); err != nil {
		sentry.CaptureException(err)
		return nil, err
	}

	// Purge from CF cache
	go func() {
		// Get token, zone ID, and URL
		token := os.Getenv("CF_TOKEN")
		zoneId := os.Getenv("CF_ZONE_ID")
		url := os.Getenv("CF_URL")
		if token == "" || zoneId == "" || url == "" {
			return
		}

		// Create file URLs
		fileUrls := []string{}
		if f.Bucket == "icons" {
			fileUrls = append(fileUrls, fmt.Sprint(url, "/icons/", f.Id))
		} else if f.Bucket == "attachments" {
			fileUrls = append(fileUrls, fmt.Sprint(url, "/attachments/", f.Id, "/", f.Filename))
			fileUrls = append(fileUrls, fmt.Sprint(url, "/attachments/", f.Id, "/", f.Filename, "?preview"))
			fileUrls = append(fileUrls, fmt.Sprint(url, "/attachments/", f.Id, "/", f.Filename, "?download"))
			fileUrls = append(fileUrls, fmt.Sprint(url, "/attachments/", f.Id, "/", f.Filename, "?preview&download"))
			fileUrls = append(fileUrls, fmt.Sprint(url, "/attachments/", f.Id, "/", f.Filename, "?download&preview"))
		}

		// Create body
		jsonBody, err := json.Marshal(map[string][]string{
			"files": fileUrls,
		})
		if err != nil {
			sentry.CaptureException(err)
			return
		}

		// Create request
		apiUrl := fmt.Sprint("https://api.cloudflare.com/client/v4/zones/", zoneId, "/purge_cache")
		req, err := http.NewRequest(http.MethodPost, apiUrl, bytes.NewReader(jsonBody))
		if err != nil {
			sentry.CaptureException(err)
			return
		}
		req.Header.Add("Authorization", fmt.Sprint("Bearer ", token))

		// Send request
		_, err = http.DefaultClient.Do(req)
		if err != nil {
			sentry.CaptureException(err)
			return
		}
	}()

	return &emptypb.Empty{}, nil
}

func (s grpcUploadsServer) ClearFiles(ctx context.Context, req *pb.ClearFilesReq) (*emptypb.Empty, error) {
	// Check token
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("x-token")) == 0 || md.Get("x-token")[0] != os.Getenv("GRPC_UPLOADS_TOKEN") {
		return nil, ErrUnauthorized
	}

	// Mark files as unclaimed
	// Files will be deleted when the cleanup thread runs
	_, err := db.Collection("files").UpdateMany(
		context.TODO(),
		bson.M{"uploaded_by": req.UserId},
		bson.M{"$set": bson.M{"claimed": false}},
	)
	return &emptypb.Empty{}, err
}
