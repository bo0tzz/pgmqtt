package mqtt

import "testing"

func TestValidateTopicName(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		in    string
		valid bool
	}{
		{"a", true},
		{"a/b/c", true},
		{"$SYS/foo", true},
		{"", false},
		{"a/+/c", false},
		{"a/#", false},
		{"a/\x00/b", false},
	} {
		err := ValidateTopicName(c.in)
		if c.valid && err != nil {
			t.Errorf("ValidateTopicName(%q): unexpected err %v", c.in, err)
		}
		if !c.valid && err == nil {
			t.Errorf("ValidateTopicName(%q): expected err", c.in)
		}
	}
}

func TestValidateTopicFilter(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		in    string
		valid bool
	}{
		{"a", true},
		{"a/b", true},
		{"a/+/b", true},
		{"+", true},
		{"#", true},
		{"a/#", true},
		{"+/+", true},
		{"+/+/#", true},
		{"", false},
		{"a/#/b", false},
		{"a/foo+", false},
		{"a/+foo", false},
		{"a#", false},
	} {
		err := ValidateTopicFilter(c.in)
		if c.valid && err != nil {
			t.Errorf("ValidateTopicFilter(%q): unexpected err %v", c.in, err)
		}
		if !c.valid && err == nil {
			t.Errorf("ValidateTopicFilter(%q): expected err", c.in)
		}
	}
}

func TestSharedSubscription(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in    string
		group string
		flt   string
		ok    bool
	}{
		{"$share/g/a/b", "g", "a/b", true},
		{"$share/group1/topic", "group1", "topic", true},
		{"$share/", "", "$share/", false},
		{"$share/g", "", "$share/g", false},
		{"a/b", "", "a/b", false},
	}
	for _, c := range cases {
		g, f, ok := SharedSubscription(c.in)
		if g != c.group || f != c.flt || ok != c.ok {
			t.Errorf("SharedSubscription(%q) = (%q,%q,%v), want (%q,%q,%v)",
				c.in, g, f, ok, c.group, c.flt, c.ok)
		}
	}
}
