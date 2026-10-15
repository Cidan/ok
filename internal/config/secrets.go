package config

import (
	"reflect"
	"sort"
	"strings"
)

// SecretPaths are the files and directories the configuration names that may hold secrets —
// every file, *_file, and *_dir but a CA's certificate (ca_file), and the TLS key — which a worker
// hides from what a change's code runs (docs/automation.md, "Execution").
func (c *Config) SecretPaths() []string {
	seen := map[string]bool{}
	var walk func(v reflect.Value)
	walk = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if !v.IsNil() {
				walk(v.Elem())
			}
		case reflect.Struct:
			t := v.Type()
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if !f.IsExported() {
					continue
				}
				tag, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
				fv := v.Field(i)
				if fv.Kind() == reflect.String && secretTag(tag) {
					if p := fv.String(); p != "" {
						seen[p] = true
					}
					continue
				}
				walk(fv)
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i))
			}
		case reflect.Map:
			for _, k := range v.MapKeys() {
				walk(v.MapIndex(k))
			}
		}
	}
	walk(reflect.ValueOf(c))
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// secretTag reports whether a field, by its YAML name, names a secret's file or directory.
func secretTag(tag string) bool {
	switch {
	case tag == "ca_file":
		return false
	case tag == "key", tag == "file":
		return true
	}
	return strings.HasSuffix(tag, "_file") || strings.HasSuffix(tag, "_dir")
}
