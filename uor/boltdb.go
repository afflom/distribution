package uor

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
)

type AttributeSet map[string][]json.RawMessage

type ResultSet []*Result

type Result struct {
	Schema    string              `json:"version"`
	AttribKey string              `json:"attribkey"`
	AttribVal json.RawMessage     `json:"attribval"`
	Digest    digest.Digest       `json:"digest"`
	Location  map[string][]string `json:"location"`
}

type Links map[digest.Digest]Target

type Target struct {
	Target digest.Digest       `json:"target"`
	Hints  map[string][]string `json:"hints"`
}

func LinkQuery(digests []string, db *bolt.DB) (Links, error) {
	links := make(Links)
	if len(digests) == 0 {
		return links, nil
	}

	if err := db.View(func(tx *bolt.Tx) error {
		linksBucket := tx.Bucket([]byte("links"))
		if linksBucket == nil {
			return fmt.Errorf("links not found")
		}

		for _, ld := range digests {
			dgst, err := digest.Parse(ld)
			if err != nil {
				return err
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

				registryCursor := linkerBucket.Cursor()
				hints := make(map[string][]string)

				for r, _ := registryCursor.First(); r != nil; r, _ = registryCursor.Next() {
					registryBucket := linkerBucket.Bucket(r)
					log.Printf("In namespace bucket %v", string(r))

					namespaceCursor := registryBucket.Cursor()

					for n, _ := namespaceCursor.First(); n != nil; n, _ = namespaceCursor.Next() {
						log.Printf("namespace found: %v", string(n))

						hints[string(r)] = append(hints[string(r)], string(n))

						hn, err := json.Marshal(hints)
						if err != nil {
							return err
						}
						log.Printf("hints %v", string(hn))
					}
				}
				links[parsedDigest] = Target{
					Target: dgst,
					Hints:  hints,
				}

			}
		}
		return nil
	}); err != nil {
		return links, err
	}
	return links, nil

}

func DigestQuery(digests []string, db *bolt.DB) ([]Target, error) {
	var target []Target

	if len(digests) == 0 {
		return target, nil
	}

	if err := db.View(func(tx *bolt.Tx) error {
		digestsBucket := tx.Bucket([]byte("digests"))
		if digestsBucket == nil {
			return fmt.Errorf("digests not found")
		}

		for _, ld := range digests {
			digest, err := digest.Parse(ld)
			if err != nil {
				return err
			}
			digestBucket := digestsBucket.Bucket([]byte(digest))
			if digestBucket == nil {
				return fmt.Errorf("digest not found")
			}

			namespaceCursor := digestBucket.Cursor()

			isEmpty := namespaceCursor.Bucket().Stats()
			log.Printf("is empty ? %v", isEmpty)
			for n, _ := namespaceCursor.First(); n != nil; n, _ = namespaceCursor.Next() {
				hints := make(map[string][]string)
				hints["local"] = append(hints["local"], string(n))
				local := Target{
					Target: digest,
					Hints:  hints,
				}
				target = append(target, local)
				log.Printf("Resolved digest %v to: %v", digest, string(n))
			}

		}
		return nil
	}); err != nil {
		return target, err
	}
	return target, nil

}

// Attribute returns a ResulSet from an attribute Query of boltdb
func AttributeQuery(attribquery map[string]json.RawMessage, db *bolt.DB) (ResultSet, error) {

	attribs := make(AttributeSet)

	for k, v := range attribquery {

		attribs[k] = append(attribs[k], v)
	}

	var resultSet ResultSet

	log.Printf("Attribs: %s", attribs)
	for schemaid, attributeValues := range attribs {
		log.Printf("Key: %s, Val: %s", schemaid, attributeValues)
		// Open a new read-only transaction
		if err := db.View(func(tx *bolt.Tx) error {
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
						result := Result{
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

func WriteDB(manifest distribution.Manifest, digest digest.Digest, repo distribution.Repository, db *bolt.DB) error {

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

		if err := db.Update(func(tx *bolt.Tx) error {
			log.Println("started update transaction")
			// Create schema ID bucket
			schemaBucket, err := tx.CreateBucketIfNotExists([]byte(schemaid))
			if err != nil {
				return err
			}
			log.Printf("Wrote schema: %s", schemaid)

			var data map[string]interface{}

			for _, pair := range attributes {

				json.Unmarshal(pair, &data)

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
			//     - Nested bucket: namespace

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
			_, err = digestBucket.CreateBucketIfNotExists([]byte(repo.Named().Name()))
			if err != nil {
				return err
			}

			// The is the schema for the link query partition
			// - Top level bucket: "links"
			//   - Nested bucket: digest (link target)
			//     - Nested bucket: digest (parent)
			//       - Nested bucket: registryHint
			//         - Nested bucket: namespaceHint

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

					log.Println("in linker bucket")

					var registry string
					var namespace string

					var ldata map[string]interface{}
					if attribs != nil {
						hints := linkAttribs["core-link"]
						log.Println("Starting hints if")
						if hints != nil {
							for _, hint := range hints {

								err = json.Unmarshal(hint, &ldata)
								if err != nil {
									return err
								}
								ld, _ := json.Marshal(ldata)
								log.Printf("ldata: %v", string(ld))

								if data["registryHint"] != "" {
									registryHint, err := json.Marshal(data["registryHint"])
									if err != nil {
										return err
									}
									registry = string(registryHint)
								}
								if data["registryHint"] == "" {

									registry = "none"
								}
								if data["namespaceHint"] != "" {
									namespaceHint, err := json.Marshal(data["namespaceHint"])
									if err != nil {
										return err
									}
									namespace = string(namespaceHint)
								}
								if data["namespaceHint"] == "" {
									namespace = "none"
								}
							}
						} else {
							log.Printf("No hints")
							namespace = "none"
							registry = "none"
						}

					} else {
						log.Printf("No hints")
						namespace = "none"
						registry = "none"
					}

					registryHintBucket, err := linkerBucket.CreateBucketIfNotExists([]byte(registry))
					if err != nil {
						return err
					}

					log.Println("in registry bucket")

					_, err = registryHintBucket.CreateBucketIfNotExists([]byte(namespace))
					if err != nil {
						return err
					}
					log.Println("in namespace bucket")

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

// tempfile returns a temporary file path.
// todo(afflom): create the database file in the /v3/ directory adjacent to the /v2/ directory
func Tempfile() string {
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
			key = "ai"
		}
		specAttributes[key] = append(specAttributes[key], jsonValue)
	}

	return specAttributes, nil
}
