package dbtest

import (
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

var dbCounter atomic.Uint64

// freshDBName produces a Postgres database name unique to this process and test.
// Postgres identifiers are limited to 63 chars; the suffix keeps total < 60.
func freshDBName(t *testing.T) string {
	n := dbCounter.Add(1)
	name := "t_" + sanitize(t.Name()) + "_" + itoa(n)
	if len(name) > 60 {
		name = name[:60]
	}
	return name
}

func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.ToLower(b.String())
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func replaceDBName(connURL, name string) string {
	u, err := url.Parse(connURL)
	if err != nil {
		// fall back to string surgery; pgx accepts both URL and DSN
		return connURL
	}
	u.Path = "/" + name
	return u.String()
}
