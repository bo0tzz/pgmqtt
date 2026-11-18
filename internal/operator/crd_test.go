package operator_test

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
)

const (
	chartCRDPath = "../../deploy/helm/pgmqtt/templates/crd-users.yaml"
	plainCRDPath = "../../config/crd/bases/pgmqtt.io_users.yaml"
)

// loadCRD parses a CRD manifest. The chart copy is a Helm template; its
// only directives are the `crds.install` guard, which is dropped here. Any
// other directive fails the parse, which is the signal to teach this
// function about it rather than let the comparison go quiet.
func loadCRD(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var kept []string
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "{{- if .Values.crds.install -}}" || trimmed == "{{- end }}" {
			continue
		}
		if strings.Contains(line, "{{") {
			t.Fatalf("%s: unexpected template directive %q", path, trimmed)
		}
		kept = append(kept, line)
	}
	asJSON, err := yaml.ToJSON([]byte(strings.Join(kept, "\n")))
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out map[string]any
	if err := json.Unmarshal(asJSON, &out); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return out
}

func dig(t *testing.T, v any, path ...any) any {
	t.Helper()
	for _, step := range path {
		switch key := step.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok || m[key] == nil {
				t.Fatalf("CRD has no %q on the way to %v", key, path)
			}
			v = m[key]
		case int:
			s, ok := v.([]any)
			if !ok || len(s) <= key {
				t.Fatalf("CRD has no index %d on the way to %v", key, path)
			}
			v = s[key]
		}
	}
	return v
}

// TestCRDCopiesAreIdentical keeps the plain manifest under config/crd in
// step with the one the chart installs. The plain copy once fell behind
// (no status.observedUsername, no username validation); applied to a
// cluster, it made the API server drop fields the operator depends on.
func TestCRDCopiesAreIdentical(t *testing.T) {
	chart, plain := loadCRD(t, chartCRDPath), loadCRD(t, plainCRDPath)

	for _, field := range []string{"apiVersion", "kind", "spec"} {
		if !reflect.DeepEqual(chart[field], plain[field]) {
			c, _ := json.MarshalIndent(chart[field], "", "  ")
			p, _ := json.MarshalIndent(plain[field], "", "  ")
			t.Errorf("%s differs between %s and %s.\nchart:\n%s\nplain:\n%s", field, chartCRDPath, plainCRDPath, c, p)
		}
	}
	if c, p := dig(t, chart, "metadata", "name"), dig(t, plain, "metadata", "name"); c != p {
		t.Errorf("metadata.name differs: chart %v, plain %v", c, p)
	}
}

// TestCRDSchemaCoversGoTypes compares the field names of UserSpec and
// UserStatus with the properties the CRD schema declares. A field the
// schema lacks is pruned by the API server on every write: the operator
// would set it, read it back empty, and never notice.
func TestCRDSchemaCoversGoTypes(t *testing.T) {
	schema := dig(t, loadCRD(t, chartCRDPath), "spec", "versions", 0, "schema", "openAPIV3Schema", "properties")

	for section, typ := range map[string]reflect.Type{
		"spec":   reflect.TypeOf(pgmqttv1alpha1.UserSpec{}),
		"status": reflect.TypeOf(pgmqttv1alpha1.UserStatus{}),
	} {
		var inGo []string
		for i := 0; i < typ.NumField(); i++ {
			name, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
			if name != "" && name != "-" {
				inGo = append(inGo, name)
			}
		}
		var inSchema []string
		for name := range dig(t, schema, section, "properties").(map[string]any) {
			inSchema = append(inSchema, name)
		}
		sort.Strings(inGo)
		sort.Strings(inSchema)
		if !reflect.DeepEqual(inGo, inSchema) {
			t.Errorf("%s fields: Go type has %v, CRD schema has %v", section, inGo, inSchema)
		}
	}
}
