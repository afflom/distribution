package uor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/distribution/distribution/v3"
	digest "github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"
)

type AttributeSet map[string][]json.RawMessage

type ResultSet []Result

type Result struct {
	Schema    string          `json:"version"`
	AttribKey string          `json:"attribkey"`
	AttribVal json.RawMessage `json:"attribval"`
	Namespace string          `json:"namespace"`
	Digest    digest.Digest   `json:"digest"`
}

// ReadDB returns a ResulSet from an attribute Query of boltdb
func ReadDB(attribquery map[string]json.RawMessage, db *bolt.DB) ResultSet {

	attribs := make(AttributeSet)

	for k, v := range attribquery {

		attribs[k] = append(attribs[k], v)
	}

	var resultSet []Result

	log.Printf("Attribs: %s", attribs)
	for attributeKey, attributeValues := range attribs {
		log.Printf("Key: %s, Val: %s", attributeKey, attributeValues)
		// Open a new read-only transaction
		if err := db.View(func(tx *bolt.Tx) error {
			log.Printf("Transaction Started")
			// Parse the schema ID from the key name
			keyname := strings.Split(attributeKey, ".")

			// Enter the schema id bucket
			schemaBucket := tx.Bucket([]byte(keyname[0]))
			// Enter the key name bucket
			attributeKeyBucket := schemaBucket.Bucket([]byte(keyname[1]))
			for _, attributeValue := range attributeValues {
				// Enter the value bucket
				valueBucket := attributeKeyBucket.Bucket([]byte(attributeValue))
				// query for kv pairs in the value bucket
				c := valueBucket.Cursor()
				// cursor loop returns namespace and digest of the match
				for namespace, d := c.First(); d != nil; d, namespace = c.Next() {
					fmt.Printf("A %s is %s.\n", d, namespace)
					digest, _ := digest.Parse(string(d))
					result := Result{
						AttribKey: attributeKey,
						AttribVal: attributeValue,
						Namespace: string(namespace),
						Digest:    digest,
					}
					resultSet = append(resultSet, result)
				}
				//fmt.Printf("Attribute: %s, Value:  %s, Manifest: %s", ak.Get([]byte(k)), av, value.Get([]byte(digest)))
			}
			return nil
		}); err != nil {
			log.Fatal(err)
		}
	}
	db.Close()
	return resultSet
}

func WriteDB(manifest distribution.Manifest, digest digest.Digest, repo distribution.Repository, db bolt.DB) error {

	// Get manifest
	_, payload, err := manifest.Payload()
	if err != nil {
		log.Fatal(err)
	}

	man := ocispec.Manifest{}

	if err := json.Unmarshal(payload, &man); err != nil {
		log.Fatal(err)
	}

	attribs := make(map[string][]json.RawMessage)

	// Parse the manifest and store each attribute as an entry in the map

	// Get manifest attributes
	for k, v := range man.Annotations {
		// TODO(afflom): This key name needs to come from the UOR spec
		if k == "uor.attributes" {
			attribs, err = convertAnnotationsToAttributes(v)
			if err != nil {
				log.Fatal(err)
			}

		} else {
			attribs[k] = append(attribs[k], []byte(v))
		}
	}

	// Get manifest's blob attributes
	for _, descriptor := range man.Layers {
		for k, v := range descriptor.Annotations {
			// TODO(afflom): This key name needs to come from the UOR spec
			if k == "uor.attributes" {
				attribs, err = convertAnnotationsToAttributes(v)
				if err != nil {
					log.Fatal(err)
				}

			} else {
				attribs[k] = append(attribs[k], []byte(v))
			}
		}
	}

	// Write attributes to database
	// Database schema:
	// - Top level bucket: SchemaID
	//   - Nested bucket: attribute key
	//     - Nested bucket: attribute value (json.RawMessage)
	//       - Key/val pair: namespace=digest

	for attributeKey, attributeValues := range attribs {

		if err := db.Update(func(tx *bolt.Tx) error {

			// parse key for schema id

			keyname := strings.Split(attributeKey, ".")
			log.Printf("Top Level Bucket: %s", keyname[0])

			// Create schema ID bucket
			schemaBucket, err := tx.CreateBucketIfNotExists([]byte(keyname[0]))

			if err != nil {
				log.Fatal(err)
			}
			log.Printf("key Bucket: %s", keyname[1])

			// Create attribute key bucket
			keyBucket, err := schemaBucket.CreateBucketIfNotExists([]byte(keyname[1]))
			if err != nil {
				log.Fatal(err)
			}

			for _, attributeValue := range attributeValues {
				// Create value bucket
				valueBucket, err := keyBucket.CreateBucketIfNotExists([]byte(attributeValue))
				log.Printf("value Bucket: %s", attributeValue)

				if err != nil {
					log.Fatal(err)
				}
				// write namespace and digest of manifest
				err = valueBucket.Put([]byte(repo.Named().Name()), []byte(digest))
				log.Printf("Writing %s as %s", repo.Named().Name(), digest)
				// Cursor produces debug output to server logs
				c := valueBucket.Cursor()
				for d, r := c.First(); d != nil; d, r = c.Next() {
					fmt.Printf(" %s is %s.\n", d, r)
				}

				if err != nil {
					log.Fatal(err)
				}
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
		specAttributes[key] = append(specAttributes[key], jsonValue)
	}

	return specAttributes, nil
}
