package manifest

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

// ResourceDefinition represents a single Kubernetes resource definition.
type ResourceDefinition struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Origin string `yaml:"-"`
	Raw    []byte `yaml:"-"`
}

var fnRe = regexp.MustCompile(`[^\w\-\.]`)

func safeFn(input string) string {
	safe := fnRe.ReplaceAllString(input, "_")
	safe = strings.Trim(safe, "._")
	return safe
}

// Filename returns a filename compatible name of the resource definition.
func (rd *ResourceDefinition) Filename() string {
	if strings.HasSuffix(rd.Origin, ".yaml") || strings.HasSuffix(rd.Origin, ".yml") {
		return path.Base(rd.Origin)
	}

	if rd.Metadata.Name != "" {
		return fmt.Sprintf("%s-%s.yaml", safeFn(rd.Kind), safeFn(rd.Metadata.Name))
	}

	return fmt.Sprintf("%s-%s-%d.yaml", safeFn(rd.APIVersion), safeFn(rd.Kind), time.Now().UnixNano())
}

// returns a Reader that reads the raw resource definition
func (rd *ResourceDefinition) Reader() *bytes.Reader {
	return bytes.NewReader(rd.Raw)
}

// Bytes returns the raw resource definition.
func (rd *ResourceDefinition) Bytes() []byte {
	return rd.Raw
}

// Unmarshal unmarshals the raw resource definition into the provided object.
func (rd *ResourceDefinition) Unmarshal(obj any) error {
	if err := yaml.UnmarshalStrict(rd.Bytes(), obj); err != nil {
		return fmt.Errorf("failed to unmarshal %s: %w", rd.Origin, err)
	}
	return nil
}

// Reader reads Kubernetes resource definitions from input streams.
type Reader struct {
	IgnoreErrors bool
	manifests    []*ResourceDefinition
}

// NamedReader is an io.Reader that carries a human-friendly name.
// It can be used to provide a source name for manifests when parsing
// from non-file readers (e.g., strings or network streams).
type NamedReader struct {
	R    io.Reader
	Name string
}

// Read implements io.Reader.
func (nr *NamedReader) Read(p []byte) (int, error) {
	if nr == nil || nr.R == nil {
		return 0, io.EOF
	}
	return nr.R.Read(p)
}

func NewNamedReader(r io.Reader, name string) *NamedReader {
	return &NamedReader{R: r, Name: name}
}

// ReaderWithName allows custom readers to expose a Name for origin tracking.
// If a reader implements this interface, its Name() will be used.
type ReaderWithName interface {
	Name() string
}

func name(r io.Reader) string {
	switch v := r.(type) {
	case *os.File:
		return v.Name()
	case ReaderWithName:
		return v.Name()
	case *NamedReader:
		return v.Name
	default:
		return "manifest"
	}
}

// Parse parses Kubernetes resource definitions from the provided input stream. They are then available via the Resources() or GetResources(apiVersion, kind) methods.
func (r *Reader) Parse(input io.Reader) error {
	yamlReader := yamlutil.NewYAMLReader(bufio.NewReader(input))

	for {
		rawChunk, err := yamlReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("error reading input: %w", err)
		}

		if len(bytes.TrimSpace(rawChunk)) == 0 {
			continue
		}

		rd := &ResourceDefinition{}
		if err := yaml.Unmarshal(rawChunk, rd); err != nil {
			if r.IgnoreErrors {
				continue
			}
			return fmt.Errorf("failed to decode resource %s: %w", name(input), err)
		}

		if rd.APIVersion == "" || rd.Kind == "" {
			if r.IgnoreErrors {
				continue
			}
			return fmt.Errorf("missing apiVersion or kind in resource %s: %w", name(input), err)
		}

		// Store the raw chunk
		rd.Origin = name(input)
		rd.Raw = append([]byte{}, rawChunk...)
		r.manifests = append(r.manifests, rd)
	}

	return nil
}

// ParseString parses Kubernetes resource definitions from the provided string.
func (r *Reader) ParseString(input string) error {
	return r.Parse(strings.NewReader(input))
}

// ParseBytes parses Kubernetes resource definitions from the provided byte slice.
func (r *Reader) ParseBytes(input []byte) error {
	return r.Parse(bytes.NewReader(input))
}

// Resources returns all parsed Kubernetes resource definitions.
func (r *Reader) Resources() []*ResourceDefinition {
	return r.manifests
}

// Len returns the number of parsed Kubernetes resource definitions.
func (r *Reader) Len() int {
	return len(r.manifests)
}

// FilterResources returns all parsed Kubernetes resource definitions that match the provided filter function.
func (r *Reader) FilterResources(filter func(rd *ResourceDefinition) bool) []*ResourceDefinition {
	var resources []*ResourceDefinition
	for _, rd := range r.manifests {
		if filter(rd) {
			resources = append(resources, rd)
		}
	}
	return resources
}

// GetResources returns all parsed Kubernetes resource definitions that match the provided apiVersion and kind. The matching is case-insensitive.
func (r *Reader) GetResources(apiVersion, kind string) ([]*ResourceDefinition, error) {
	resources := r.FilterResources(func(rd *ResourceDefinition) bool {
		return strings.EqualFold(rd.APIVersion, apiVersion) && strings.EqualFold(rd.Kind, kind)
	})

	if len(resources) == 0 {
		return nil, fmt.Errorf("no resources found for apiVersion=%s, kind=%s", apiVersion, kind)
	}
	return resources, nil
}
