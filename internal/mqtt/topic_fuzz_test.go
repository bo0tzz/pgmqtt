package mqtt

import (
	"strings"
	"testing"
)

// FuzzValidateTopicName asserts the topic-name validator never panics
// on arbitrary input. The matching seed corpus exercises the three
// classes ValidateTopicName accepts/rejects: bare names, names with
// wildcards (rejected), and bad-UTF-8 / NUL.
func FuzzValidateTopicName(f *testing.F) {
	for _, s := range []string{
		"",
		"a",
		"a/b",
		"a/b/c",
		"a/+/c",         // wildcard — should reject (publish topic)
		"a/#",           // wildcard — should reject
		"\x00",          // NUL — should reject
		strings.Repeat("a", 65535), // upper bound
		strings.Repeat("a", 65536), // over bound — should reject
		"日本語",
		"\xff\xfe",      // bad UTF-8
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// Must return nil or one of the documented errors. Must not
		// panic. Must terminate. (Length is bounded by the fuzzer's
		// own MaxLength; if larger we still expect rejection, not
		// runaway compute.)
		err := ValidateTopicName(s)
		if err != nil && err != ErrInvalidTopicName {
			t.Fatalf("validate %q: unexpected error %v", s, err)
		}
	})
}

// FuzzValidateTopicFilter asserts the filter validator never panics on
// arbitrary input. Filters allow '+' and '#' under structural rules.
func FuzzValidateTopicFilter(f *testing.F) {
	for _, s := range []string{
		"",
		"a",
		"a/b",
		"a/+/b",
		"+",
		"#",
		"a/#",
		"a/+",
		"a/#/b",         // # not last — reject
		"a+b",           // + mixed with chars — reject
		"a#",            // # in non-terminal level — reject
		"$share/g/t",
		"$share/g/+/t",
		strings.Repeat("a/+/", 16384) + "#",
		"\x00",
		"\xff\xfe",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		err := ValidateTopicFilter(s)
		if err != nil && err != ErrInvalidTopicFilter {
			t.Fatalf("validate filter %q: unexpected error %v", s, err)
		}
	})
}

// FuzzSharedSubscription asserts the $share/group/filter parser is
// total — it must produce a (group, filter, ok) tuple for any input
// without panicking. The audit flagged this as a Paho-untested surface.
func FuzzSharedSubscription(f *testing.F) {
	for _, s := range []string{
		"",
		"a",
		"$share/",
		"$share/g/t",
		"$share/g/+/t",
		"$share/g/#",
		"$share//empty",
		"$share/group-only-no-filter",
		strings.Repeat("$share/g/", 100) + "t",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		group, filter, ok := SharedSubscription(s)
		if ok {
			// When ok, the recombined form must equal the input.
			if got := "$share/" + group + "/" + filter; got != s {
				t.Fatalf("round-trip mismatch: got %q from %q", got, s)
			}
		} else {
			// When !ok, filter must equal the input (pass-through).
			if filter != s {
				t.Fatalf("non-shared filter mismatch: got %q from %q", filter, s)
			}
		}
	})
}
