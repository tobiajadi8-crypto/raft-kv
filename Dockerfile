FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /raftkv .
FROM alpine:3.23
RUN adduser -D -u 10001 raft && mkdir /data && chown raft /data
COPY --from=build /raftkv /usr/local/bin/raftkv
USER raft
ENTRYPOINT ["raftkv"]
