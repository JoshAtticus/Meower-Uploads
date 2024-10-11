# Build stage
FROM golang AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o /Meower-Uploads

# Production stage
FROM ubuntu:24.04
COPY --from=builder /Meower-Uploads /Meower-Uploads
ENTRYPOINT ["/Meower-Uploads"]
