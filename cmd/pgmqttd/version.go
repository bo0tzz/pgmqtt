package main

// version is stamped at build time with -ldflags "-X main.version=..."
// (Makefile, Dockerfile). A plain `go build` or `go run` leaves it at "dev".
var version = "dev"
