package metadata

import (
	"encoding/json"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/distribution/distribution/v3"
)

// Indexer define required storage and searching methods for a metadata indexer.
type Indexer interface {
	IngestMetadata(manifest distribution.Manifest, digest digest.Digest, repository distribution.Repository) error
	SearchByAttribute(attributes map[string]json.RawMessage) (ResultSet, error)
	SearchByDigest(digests []string) ([]ocispec.Descriptor, error)
	SearchByLink(digests []string) (Links, error)
}

type ResultSet []*Result

type Result struct {
	Schema    string              `json:"version"`
	AttribKey string              `json:"attribkey"`
	AttribVal json.RawMessage     `json:"attribval"`
	Digest    digest.Digest       `json:"digest"`
	Location  map[string][]string `json:"location"`
}

type Links map[digest.Digest][]ocispec.Descriptor
