package mqtt

import (
	"errors"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalidTopicName indicates a publish topic name violates MQTT-4.7.
	ErrInvalidTopicName = errors.New("mqtt: invalid topic name")
	// ErrInvalidTopicFilter indicates a subscription filter violates MQTT-4.7.
	ErrInvalidTopicFilter = errors.New("mqtt: invalid topic filter")
)

// ValidateTopicName validates a publish topic name (no wildcards allowed).
//
// Rules (MQTT-4.7.1):
//   - Non-empty
//   - Valid UTF-8, no NUL byte
//   - Length 1..65535
//   - Must not contain '+' or '#'
func ValidateTopicName(t string) error {
	if t == "" || len(t) > 65535 {
		return ErrInvalidTopicName
	}
	if !utf8.ValidString(t) {
		return ErrInvalidTopicName
	}
	if strings.ContainsAny(t, "+#\x00") {
		return ErrInvalidTopicName
	}
	return nil
}

// ValidateTopicFilter validates a subscription filter (wildcards allowed).
//
// Rules (MQTT-4.7.1):
//   - Non-empty
//   - Valid UTF-8, no NUL byte
//   - Length 1..65535
//   - '+' must occupy a complete level: "a/+/b" ok; "a+" or "a/foo+" not.
//   - '#' must be the final character and occupy a complete level: "a/#" ok;
//     "#" alone ok; "a/#/b" or "a#" not.
func ValidateTopicFilter(t string) error {
	if t == "" || len(t) > 65535 {
		return ErrInvalidTopicFilter
	}
	if !utf8.ValidString(t) {
		return ErrInvalidTopicFilter
	}
	if strings.ContainsRune(t, '\x00') {
		return ErrInvalidTopicFilter
	}
	parts := strings.Split(t, "/")
	for i, p := range parts {
		if strings.Contains(p, "#") && p != "#" {
			return ErrInvalidTopicFilter
		}
		if strings.Contains(p, "+") && p != "+" {
			return ErrInvalidTopicFilter
		}
		if p == "#" && i != len(parts)-1 {
			return ErrInvalidTopicFilter
		}
	}
	return nil
}

// SharedSubscription returns ("group", "filter", true) if t is a shared
// subscription of the form $share/{group}/{filter}, else ("", t, false).
//
// We accept and parse the form so SUBSCRIBE doesn't fail outright, but the
// engine currently treats the underlying filter as a normal subscription
// (i.e. shared semantics are not implemented for v1).
func SharedSubscription(t string) (group, filter string, ok bool) {
	if !strings.HasPrefix(t, "$share/") {
		return "", t, false
	}
	rest := t[len("$share/"):]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return "", t, false
	}
	return rest[:slash], rest[slash+1:], true
}
