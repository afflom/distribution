package uor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/distribution/distribution/v3"
	digest "github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"
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

func LinkQuery(digests []string, db *bolt.DB) (links Links, err error) {

	if err := db.View(func(tx *bolt.Tx) error {
		linksBucket := tx.Bucket([]byte("links"))
		for _, ld := range digests {
			if linksBucket == nil {
				return err
			}
			digest := digest.FromString(ld)
			targetBucket := linksBucket.Bucket([]byte(digest))

			linkerCursor := targetBucket.Cursor()
			for l, _ := linkerCursor.First(); l != nil; l, _ = linkerCursor.Next() {
				parsedDigest := digest.Algorithm().FromBytes(l)
				registryBucket := targetBucket.Bucket(l)
				registryCursor := targetBucket.Cursor()
				hints := make(map[string][]string)

				for r, _ := registryCursor.First(); r != nil; r, _ = registryCursor.Next() {
					namespaceBucket := registryBucket.Bucket(r)
					namespaceCursor := namespaceBucket.Cursor()

					for n, _ := namespaceCursor.First(); n != nil; n, _ = namespaceCursor.Next() {
						hints[string(r)] = append(hints[string(r)], string(n))
					}
				}
				links[parsedDigest] = Target{
					Target: digest,
					Hints:  hints,
				}

			}
		}
		return nil
	}); err != nil {
		return nil, nil
	}
	return nil, nil

}

func DigestQuery(digests []string, db *bolt.DB) (resolvedDigests map[digest.Digest][]string, err error) {

	if err := db.View(func(tx *bolt.Tx) error {
		digestsBucket := tx.Bucket([]byte("digests"))
		for _, ld := range digests {
			digest := digest.FromString(ld)

			if digestsBucket == nil {
				return fmt.Errorf("No matching records found for schema: %v", digest.String())
			}
			namespaceCursor := digestsBucket.Cursor()
			for n, _ := namespaceCursor.First(); n != nil; n, _ = namespaceCursor.Next() {
				resolvedDigests[digest] = append(resolvedDigests[digest], string(n))
			}

		}
		return nil
	}); err != nil {
		return nil, nil
	}
	return nil, nil

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
				return fmt.Errorf("No matching records found for schema: %v", schemaid)
			}
			log.Printf("schema: %v", schemaid)
			// Enter the key name bucket
			var data map[string]interface{}

			for i, pair := range attributeValues {
				json.Unmarshal(pair, &data)
				log.Printf("Data: %v", data)
				for keyname, value := range data {

					log.Printf("Attribute key%v: %v", i, keyname)
					attributeKeyBucket := schemaBucket.Bucket([]byte(keyname))
					for _, attributeValue := range attributeValues {
						v, _ := json.Marshal(&value)
						log.Printf("Attribute key%v, Value: %v", i, string(v))

						// Enter the value bucket
						valueBucket := attributeKeyBucket.Bucket([]byte(v))
						// query for kv pairs in the value bucket
						digestCursor := valueBucket.Cursor()
						// cursor loop returns namespace and digest of the match
						log.Printf("bucket stats: %v", digestCursor.Bucket().Stats().KeyN)
						for d, _ := digestCursor.First(); d != nil; d, _ = digestCursor.Next() {
							log.Printf("Found Digest: %v", string(d))
							digest, _ := digest.Parse(string(d))
							result := Result{
								Schema:    schemaid,
								AttribKey: keyname,
								AttribVal: attributeValue,
								Digest:    digest,
							}
							resultSet = append(resultSet, &result)
						}
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
		log.Fatal(err)
	}
	man := ocispec.Manifest{}
	if err := json.Unmarshal(payload, &man); err != nil {
		log.Fatal(err)
	}

	// Parse the manifest and store each attribute as an entry in the map

	// Get manifest attributes and link
	log.Println("Get manifest attributes and link")

	attribs := make(map[string][]json.RawMessage)

	var link v1.Descriptor
	linkAttribs := make(map[string][]json.RawMessage)

	for k, v := range man.Annotations {
		if k == "uor.attributes" {
			attrs, err := convertAnnotationsToAttributes(v)
			if err != nil {
				log.Fatal(err)
			}
			for sid, vals := range attrs {
				attribs[sid] = append(attribs[sid], vals...)

			}

			log.Printf("writing collection attributes to manifest: %v", v)

		} else if k == "uor.link" {
			var jsonData map[string]interface{}

			if err := json.Unmarshal([]byte(v), &jsonData); err != nil {
				return err
			}

			for _, value := range jsonData {
				jsonValue, err := json.Marshal(value)
				if err != nil {
					return err
				}
				err = json.Unmarshal(jsonValue, &link)
				if err != nil {
					return err
				}
			}
			// Get link attributes

			for k, v := range link.Annotations {
				if k == "uor.attributes" {
					linkAttribs, err = convertAnnotationsToAttributes(v)
					if err != nil {
						log.Fatal(err)
					}
					// Add the attributes of the link to this manifest's attributeset
					for sid, vals := range linkAttribs {
						attribs[sid] = append(attribs[sid], vals...)

					}
				}
				log.Printf("writing collection attributes to manifest: %v", v)
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
					log.Fatal(err)
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

	// The is the schema for the attribute query partition
	// - Top level bucket: SchemaID
	//   - Nested bucket: attribute key
	//     - Nested bucket: attribute value (json.RawMessage)
	//         - Nested bucket: digest

	for schemaid, attributes := range attribs {

		if err := db.Update(func(tx *bolt.Tx) error {
			log.Println("started update transaction")
			// Create schema ID bucket
			schemaBucket, err := tx.CreateBucketIfNotExists([]byte(schemaid))
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("Wrote schema: %s", schemaid)

			var data map[string]interface{}

			for _, pair := range attributes {

				json.Unmarshal(pair, &data)

				for keyname, value := range data {

					// Create attribute key bucket
					keyBucket, err := schemaBucket.CreateBucketIfNotExists([]byte(keyname))
					if err != nil {
						log.Fatal(err)
					}
					log.Printf("Wrote Key: %s", keyname)

					v, err := json.Marshal(value)
					if err != nil {
						log.Fatal(err)
					}

					// Create value bucket
					valueBucket, err := keyBucket.CreateBucketIfNotExists([]byte(v))
					if err != nil {
						log.Fatal(err)
					}
					log.Printf("Wrote Value: %s", string(v))

					_, err = valueBucket.CreateBucketIfNotExists([]byte(digest.Encoded()))
					if err != nil {
						log.Fatal(err)
					}
					log.Printf("Wrote Digest: %s", digest.String())

					if err != nil {
						log.Fatal(err)
					}
				}
			}

			// The is the schema for the digest query partition
			// - Top level bucket: digests
			//   - Nested bucket: digest
			//     - Nested bucket: namespace

			// Write digest query database partition
			digestTopBucket, err := tx.CreateBucketIfNotExists([]byte("digests"))
			if err != nil {
				log.Fatal(err)
			}
			digestBucket, err := digestTopBucket.CreateBucketIfNotExists([]byte(digest))
			if err != nil {
				log.Fatal(err)
			}
			_, err = digestBucket.CreateBucketIfNotExists([]byte(repo.Named().Name()))
			if err != nil {
				log.Fatal(err)
			}

			// The is the schema for the link query partition
			// - Top level bucket: "links"
			//   - Nested bucket: digest (link target)
			//     - Nested bucket: digest (parent)
			//     - Nested bucket: "registry" (nested under link target digest)
			//       - Nested bucket: registryHint
			//         - Nested bucket: namespaceHint

			// Write link lookup database partition
			if link.Digest.String() != "" {
				linksTopBucket, err := tx.CreateBucketIfNotExists([]byte("links"))
				if err != nil {
					log.Fatal(err)
				}

				linkTargetBucket, err := linksTopBucket.CreateBucketIfNotExists([]byte(link.Digest))
				if err != nil {
					log.Fatal(err)
				}

				linkerBucket, err := linkTargetBucket.CreateBucketIfNotExists([]byte(digest))
				if err != nil {
					log.Fatal(err)
				}

				var ldata map[string]interface{}

				hints := linkAttribs["core-link"]

				var registry string
				var namespace string

				for _, hint := range hints {

					err := json.Unmarshal(hint, &ldata)
					if err != nil {
						log.Fatal(err)
					}
					if data["registryHint"] != "" {
						registryHint, err := json.Marshal(data["registryHint"])
						if err != nil {
							log.Fatal(err)
						}
						registry = string(registryHint)
					}
					if data["namespaceHint"] != "" {
						namespaceHint, err := json.Marshal(data["namespaceHint"])
						if err != nil {
							log.Fatal(err)
						}
						namespace = string(namespaceHint)
					}
				}

				registryHintBucket, err := linkerBucket.CreateBucketIfNotExists([]byte(registry))
				if err != nil {
					log.Fatal(err)
				}

				_, err = registryHintBucket.CreateBucketIfNotExists([]byte(namespace))
				if err != nil {
					log.Fatal(err)
				}

				return nil

			}

			return nil

		}); err != nil {
			log.Fatal(err)
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

	if err := json.Unmarshal([]byte(annotations), &jsonData); err != nil {
		return specAttributes, err
	}

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
