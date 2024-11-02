package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"crypto/tls"
	"crypto/x509"

	"github.com/getsentry/sentry-go"
	"github.com/go-chi/chi/v5"
	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
	"github.com/rs/cors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ctx context.Context = context.Background()
var db *mongo.Database
var rdb *redis.Client
var s3Clients = make(map[string]*minio.Client)
var s3RegionOrder = []string{}

func main() {
	var err error

	// Load dotenv
	godotenv.Load()

	// Initialise Sentry
	sentry.Init(sentry.ClientOptions{
		Dsn: os.Getenv("SENTRY_DSN"),
	})

	// Connect to MongoDB
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI(os.Getenv("MONGO_URI")).SetServerAPIOptions(serverAPI)
	client, err := mongo.Connect(context.TODO(), mongoOpts)
	if err != nil {
		log.Fatalln(err)
	}

	// Ping MongoDB
	var result bson.M
	if err := client.Database("admin").RunCommand(
		context.TODO(),
		bson.D{{Key: "ping", Value: 1}},
	).Decode(&result); err != nil {
		log.Fatalln(err)
	}

	// Set database
	db = client.Database(os.Getenv("MONGO_DB"))

	// Connect to Redis
	opt, err := redis.ParseURL(os.Getenv("REDIS_URI"))
	if err != nil {
		log.Fatalln(err)
	}
	rdb = redis.NewClient(opt)
	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalln(err)
	}

	// Connect to MinIO regions
	var s3Endpoints [][2]string
	err = json.Unmarshal([]byte(os.Getenv("MINIO_REGIONS")), &s3Endpoints)
	if err != nil {
		log.Fatalln(err)
	}
	for _, region := range s3Endpoints {
		name := region[0]
		endpoint := region[1]
		opts := &minio.Options{
			Creds:  credentials.NewStaticV4(os.Getenv("MINIO_ACCESS_KEY"), os.Getenv("MINIO_SECRET_KEY"), ""),
			Secure: os.Getenv("MINIO_SECURE") == "1",
		}
		if os.Getenv("MINIO_CA_CERT") != "" {
			caCert, err := os.ReadFile(os.Getenv("MINIO_CA_CERT"))
			if err != nil {
				log.Fatalln(err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			opts.Transport = &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: caCertPool,
				},
			}
		}
		s3Clients[name], err = minio.New(endpoint, opts)
		if err != nil {
			log.Fatalln(err)
		}
		s3RegionOrder = append(s3RegionOrder, name)
	}

	// Files cleanup
	go func() {
		for {
			time.Sleep(time.Minute)
			if err := cleanupFiles(); err != nil {
				sentry.CaptureException(err)
			}
		}
	}()

	// Create HTTP router
	r := chi.NewRouter()
	r.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	}).Handler)
	r.Post("/{bucket:icons|emojis|stickers|attachments}", uploadFile)
	r.Get("/{bucket:icons|emojis|stickers|attachments}/{id}", downloadFile)
	r.Get("/{bucket:icons|emojis|stickers|attachments}/{id}/*", downloadFile)

	// Send Sentry message
	sentry.CaptureMessage("Starting uploads service")

	// Serve HTTP router
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "3000"
	}
	log.Println("Serving HTTP server on :" + port)
	http.ListenAndServe(":"+port, r)

	// Wait for Sentry events to flush
	sentry.Flush(time.Second * 5)
}
