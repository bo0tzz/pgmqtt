package db_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/bo0tzz/pgmqtt/internal/db"
)

func TestScrubURLErrorReplacesPassword(t *testing.T) {
	url := "postgres://pgmqtt:supersecret123@host:5432/pgmqtt?sslmode=disable"
	in := errors.New("connect tcp 127.0.0.1:5432: " + url + ": connection refused")
	out := db.ScrubURLError(in, url)
	msg := out.Error()
	if strings.Contains(msg, "supersecret123") {
		t.Fatalf("password leaked: %q", msg)
	}
	if !strings.Contains(msg, "REDACTED") {
		t.Fatalf("expected REDACTED in scrubbed message, got %q", msg)
	}
}

func TestScrubURLErrorPasswordOnlyEmbedded(t *testing.T) {
	url := "postgres://u:p4ssw0rd@h/db"
	in := errors.New("auth failed for user u (password p4ssw0rd)")
	out := db.ScrubURLError(in, url)
	if strings.Contains(out.Error(), "p4ssw0rd") {
		t.Fatalf("password leaked: %q", out.Error())
	}
}

func TestScrubURLErrorNoPasswordPasses(t *testing.T) {
	url := "postgres://u@h/db"
	in := errors.New("some error")
	out := db.ScrubURLError(in, url)
	if out.Error() != "some error" {
		t.Fatalf("unchanged-input mutated: %q", out.Error())
	}
}

func TestScrubURLErrorNilReturnsNil(t *testing.T) {
	if db.ScrubURLError(nil, "postgres://u:p@h/db") != nil {
		t.Fatal("nil err must round-trip nil")
	}
}

func TestScrubURLErrorMalformedURLLeavesMessage(t *testing.T) {
	in := errors.New("connect: invalid URL")
	out := db.ScrubURLError(in, "::not a url::")
	if out.Error() != "connect: invalid URL" {
		t.Fatalf("malformed URL changed message: %q", out.Error())
	}
}
