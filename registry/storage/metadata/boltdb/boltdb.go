package boltdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	digest "github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/registry/storage/metadata"
)

const (
	linksBucket  = "links"
	digestBucket = "digests"
)

// Indexer is a boltdb metadata indexer.
type Indexer struct {
	rootDir string
	db      *bolt.DB
	logger  *context.Logger
}

// NewBoltDBIndexer returns a boltdb implementation of the metadata.Indexer
func NewBoltDBIndexer(path string) (metadata.Indexer, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return Indexer{
		rootDir: path,
		db:      db,
	}, nil
}

func (i Indexer) IngestMetadata(manifest distribution.Manifest, digest digest.Digest, repository distribution.Repository) error {
	log.Println("Writing to database")
	// Get manifest
	_, payload, err := manifest.Payload()
	if err != nil {
		return err
	}
	man := ocispec.Manifest{}
	if err := json.Unmarshal(payload, &man); err != nil {
		return err
	}

	// Parse the manifest and store each attribute as an entry in the map

	// Get manifest attributes and link
	log.Println("Get manifest attributes and link")

	attribs := make(map[string][]json.RawMessage)

	var links []v1.Descriptor
	linkAttribs := make(map[string][]json.RawMessage)

	for k, v := range man.Annotations {
		if k == "uor.attributes" {
			log.Println("uor attributes found")
			attrs, err := convertAnnotationsToAttributes(v)
			if err != nil {
				return err
			}
			for sid, vals := range attrs {
				attribs[sid] = append(attribs[sid], vals...)
			}

			log.Printf("writing collection attributes to manifest: %v", v)

		} else if k == "uor.link" {

			if err := json.Unmarshal([]byte(v), &links); err != nil {
				return err
			}

			// Get link attributes
			for _, link := range links {
				for k, v := range link.Annotations {
					if k == "uor.attributes" {
						linkAttribs, err = convertAnnotationsToAttributes(v)
						if err != nil {
							return err
						}

						// Add the attributes of the link to this manifest's attributeset
						for sid, vals := range linkAttribs {
							attribs[sid] = append(attribs[sid], vals...)

							ld, err := json.Marshal(sid)
							if err != nil {
								return err
							}
							log.Printf("link attribute: %v", string(ld))
						}
					}
					log.Printf("writing collection attributes to manifest: %v", v)
				}
			}
		}

	}

	// Get manifest's blob attributes
	for _, descriptor := range man.Layers {
		for k, v := range descriptor.Annotations {
			// TODO(afflom): This key name needs to come from the UOR spec
			if k == "uor.attributes" {
				attrs, err := convertAnnotationsToAttributes(v)
				if err != nil {
					return err
				}
				for sid, vals := range attrs {
					attribs[sid] = append(attribs[sid], vals...)
				}
				log.Printf("writing collection attributes to manifest: %v", v)

			} else {
				//attribs[""] = append(attribs[k], []byte(v))
				//log.Printf("writing collection attributes to manifest: %v", v)
				continue
			}
		}
	}

	log.Printf("Attributes to be written: %v", attribs)

	// This is the schema for the attribute query partition
	// - Top level bucket: SchemaID
	//   - Nested bucket: attribute key
	//     - Nested bucket: attribute value (json.RawMessage)
	//         - Nested bucket: digest
	log.Println("Writing attribute query partition")

	for schemaid, attributes := range attribs {

		if err := i.db.Update(func(tx *bolt.Tx) error {
			log.Println("started update transaction")
			// Create schema ID bucket
			schemaBucket, err := tx.CreateBucketIfNotExists([]byte(schemaid))
			if err != nil {
				return err
			}
			log.Printf("Wrote schema: %s", schemaid)

			var data map[string]interface{}

			for _, pair := range attributes {

				if err := json.Unmarshal(pair, &data); err != nil {
					return err
				}

				for keyname, value := range data {

					// Create attribute key bucket
					keyBucket, err := schemaBucket.CreateBucketIfNotExists([]byte(keyname))
					if err != nil {
						return err
					}
					log.Printf("Wrote Key: %s", keyname)

					v, err := json.Marshal(value)
					if err != nil {
						return err
					}

					// Create value bucket
					valueBucket, err := keyBucket.CreateBucketIfNotExists(v)
					if err != nil {
						return err
					}
					log.Printf("Wrote Value: %s", string(v))

					_, err = valueBucket.CreateBucketIfNotExists([]byte(digest))
					if err != nil {
						return err
					}
					log.Printf("Wrote Digest: %s", digest.String())

					if err != nil {
						return err
					}
				}
			}

			// The is the schema for the digest query partition
			// - Top level bucket: digests
			//   - Nested bucket: digest
			//     - Nested bucket: descriptor

			// Write digest query database partition
			log.Println("Writing digest query partition")

			digestTopBucket, err := tx.CreateBucketIfNotExists([]byte("digests"))
			if err != nil {
				return err
			}
			digestBucket, err := digestTopBucket.CreateBucketIfNotExists([]byte(digest))
			if err != nil {
				return err
			}
			desc := ocispec.Descriptor{
				MediaType:   man.MediaType,
				Size:        int64(len(payload)),
				Digest:      digest,
				Annotations: man.Annotations,
			}
			if desc.Annotations == nil {
				desc.Annotations = map[string]string{}
			}
			desc.Annotations["namespaceHint"] = repository.Named().Name()
			descJSON, err := json.Marshal(desc)
			if err != nil {
				return err
			}
			_, err = digestBucket.CreateBucketIfNotExists(descJSON)
			if err != nil {
				return err
			}

			// The is the schema for the link query partition
			// - Top level bucket: "links"
			//   - Nested bucket: digest (link target)
			//     - Nested bucket: digest (parent)
			//       - Nested bucket:  target descriptor

			// Write link lookup database partition

			for _, link := range links {
				log.Println("Writing link query partition")
				peek, err := json.Marshal(link)
				if err != nil {
					return err
				}
				log.Printf("link: %v", string(peek))

				if link.Digest.String() != "" {

					log.Println("link has digest")

					linksTopBucket, err := tx.CreateBucketIfNotExists([]byte("links"))
					if err != nil {
						return err
					}
					log.Println("in links bucket")

					linkTargetBucket, err := linksTopBucket.CreateBucketIfNotExists([]byte(link.Digest))
					if err != nil {
						return err
					}

					log.Println("in target bucket")

					linkerBucket, err := linkTargetBucket.CreateBucketIfNotExists([]byte(digest))
					if err != nil {
						return err
					}

					linkTargetDescriptor, err := json.Marshal(link)
					if err != nil {
						return err
					}
					_, err = linkerBucket.CreateBucketIfNotExists(linkTargetDescriptor)
					if err != nil {
						return err
					}

					return nil
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (i Indexer) SearchByAttribute(attributes map[string]json.RawMessage) (metadata.ResultSet, error) {
	attribs := make(AttributeSet)

	for k, v := range attributes {

		attribs[k] = append(attribs[k], v)
	}

	var resultSet metadata.ResultSet

	log.Printf("Attribs: %s", attribs)
	for schemaid, attributeValues := range attribs {
		log.Printf("Key: %s, Val: %s", schemaid, attributeValues)
		// Open a new read-only transaction
		if err := i.db.View(func(tx *bolt.Tx) error {
			log.Printf("Transaction Started")
			// Parse the schema ID from the key name
			// Enter the schema id bucket
			schemaBucket := tx.Bucket([]byte(schemaid))
			if schemaBucket == nil {
				return fmt.Errorf("no matching records found for schema: %v", schemaid)
			}
			log.Printf("schema: %v", schemaid)
			// Enter the key name bucket
			var data map[string]interface{}

			for i, pair := range attributeValues {
				if err := json.Unmarshal(pair, &data); err != nil {
					return err
				}
				log.Printf("Data: %v", data)
				for keyname, value := range data {

					log.Printf("Attribute key%v: %v", i, keyname)
					attributeKeyBucket := schemaBucket.Bucket([]byte(keyname))
					if attributeKeyBucket == nil {
						continue
					}
					v, err := json.Marshal(&value)
					if err != nil {
						return err
					}
					log.Printf("Attribute key%v, Value: %v", i, string(v))

					// Enter the value bucket
					valueBucket := attributeKeyBucket.Bucket(v)
					if valueBucket == nil {
						continue
					}
					// query for kv pairs in the value bucket
					digestCursor := valueBucket.Cursor()
					// cursor loop returns namespace and digest of the match
					log.Printf("bucket stats: %v", digestCursor.Bucket().Stats().KeyN)
					for d, _ := digestCursor.First(); d != nil; d, _ = digestCursor.Next() {
						log.Printf("Found Digest: %v", string(d))
						digest, err := digest.Parse(string(d))
						if err != nil {
							return err
						}
						err = digest.Validate()
						if err != nil {
							return err
						}
						result := metadata.Result{
							Schema:    schemaid,
							AttribKey: keyname,
							AttribVal: v,
							Digest:    digest,
						}
						resultSet = append(resultSet, &result)
					}

					//fmt.Printf("Attribute: %s, Value:  %s, Manifest: %s", ak.Get([]byte(k)), av, value.Get([]byte(digest)))
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return resultSet, nil
}

func (i Indexer) SearchByDigest(digests []string) ([]v1.Descriptor, error) {
	if len(digests) == 0 {
		return nil, nil
	}

	var targets []ocispec.Descriptor

	if err := i.db.View(func(tx *bolt.Tx) error {
		digestsBucket := tx.Bucket([]byte("digests"))
		if digestsBucket == nil {
			return fmt.Errorf("digests not found")
		}

		for _, ld := range digests {
			digest, err := digest.Parse(ld)
			if err != nil {
				return fmt.Errorf("error parsing digest %s: %w", ld, err)
			}
			digestBucket := digestsBucket.Bucket([]byte(digest))
			if digestBucket == nil {
				return fmt.Errorf("digest not found")
			}

			descriptorCursor := digestBucket.Cursor()
			log.Println(descriptorCursor.Bucket().Stats())
			for n, _ := descriptorCursor.First(); n != nil; n, _ = descriptorCursor.Next() {
				log.Printf("found %s", string(n))
				var desc ocispec.Descriptor
				if err := json.Unmarshal(n, &desc); err != nil {
					return err
				}
				targets = append(targets, desc)
				log.Printf("Resolved digest %v to: %v", digest, string(n))
			}

		}
		log.Println(targets)
		return nil
	}); err != nil {
		return nil, err
	}
	return targets, nil
}

func (i Indexer) SearchByLink(digests []string) (metadata.Links, error) {
	links := make(metadata.Links)
	if len(digests) == 0 {
		return links, nil
	}

	if err := i.db.View(func(tx *bolt.Tx) error {
		linksBucket := tx.Bucket([]byte("links"))
		if linksBucket == nil {
			return fmt.Errorf("links not found")
		}

		for _, ld := range digests {
			dgst, err := digest.Parse(ld)
			if err != nil {
				return fmt.Errorf("error parsing digest %s: %w", ld, err)
			}

			targetBucket := linksBucket.Bucket([]byte(dgst))
			if targetBucket == nil {
				return fmt.Errorf("digest not found")
			}

			linkerCursor := targetBucket.Cursor()
			for l, _ := linkerCursor.First(); l != nil; l, _ = linkerCursor.Next() {
				parsedDigest, err := digest.Parse(string(l))
				if err != nil {
					return err
				}
				linkerBucket := targetBucket.Bucket(l)
				log.Printf("in linker bucket: %v", string(l))

				descriptorCursor := linkerBucket.Cursor()
				var descriptors []ocispec.Descriptor

				for r, _ := descriptorCursor.First(); r != nil; r, _ = descriptorCursor.Next() {
					var descriptor ocispec.Descriptor
					if err := json.Unmarshal(r, &descriptor); err != nil {
						return err
					}
					descriptors = append(descriptors, descriptor)
				}
				links[parsedDigest] = descriptors
			}
		}
		return nil
	}); err != nil {
		return links, err
	}
	return links, nil
}

type AttributeSet map[string][]json.RawMessage

// tempFile returns a temporary file path.
// todo(afflom): create the database file in the /v3/ directory adjacent to the /v2/ directory
func tempFile() string {
	f, err := ioutil.TempFile("", "bolt-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}

func convertAnnotationsToAttributes(annotations string) (AttributeSet, error) {
	specAttributes := AttributeSet{}

	var jsonData map[string]interface{}
	log.Println("unmarshalling annotations")
	if err := json.Unmarshal([]byte(annotations), &jsonData); err != nil {
		return specAttributes, err
	}
	log.Println("unmarshaled annotations")
	for key, value := range jsonData {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return specAttributes, err
		}
		if key == "" {
			key = "unknown"
		}
		specAttributes[key] = append(specAttributes[key], jsonValue)
	}

	return specAttributes, nil
}
