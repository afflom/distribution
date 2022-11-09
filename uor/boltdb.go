package uor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

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
func ReadDB(attribquery map[string]json.RawMessage, db *bolt.DB) (ResultSet, error) {

	attribs := make(AttributeSet)

	for k, v := range attribquery {

		attribs[k] = append(attribs[k], v)
	}

	var resultSet []Result

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
							digestBucket := valueBucket.Bucket([]byte(d))
							namespaceCursor := digestBucket.Cursor()

							for n, _ := namespaceCursor.First(); n != nil; n, _ = namespaceCursor.Next() {
								log.Printf("Found Namespace: %v", string(n))

								digest, _ := digest.Parse(string(d))
								result := Result{
									Schema:    schemaid,
									AttribKey: keyname,
									AttribVal: attributeValue,
									Namespace: string(n),
									Digest:    digest,
								}
								resultSet = append(resultSet, result)
							}
						}
						//fmt.Printf("Attribute: %s, Value:  %s, Manifest: %s", ak.Get([]byte(k)), av, value.Get([]byte(digest)))
					}
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

			log.Printf("writing collection attributes to manifest: %v", v)

		} else {
			//attribs[k] = append(attribs[k], []byte(v))
			//log.Printf("writing collection attributes to manifest: %v", v)
			continue

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
				log.Printf("writing collection attributes to manifest: %v", v)

			} else {
				//attribs[""] = append(attribs[k], []byte(v))
				//log.Printf("writing collection attributes to manifest: %v", v)
				continue
			}
		}
	}

	log.Printf("Attributes to be written: %v", attribs)

	// Write attributes to database
	// Database schema:
	// - Top level bucket: SchemaID
	//   - Nested bucket: attribute key
	//     - Nested bucket: attribute value (json.RawMessage)
	//       - Nested bucket: namespace
	//         - kv pair: digest=mediatype

	for schemaid, attributes := range attribs {

		if err := db.Update(func(tx *bolt.Tx) error {
			log.Println("started update transaction")
			// parse key for schema id

			log.Printf("schemaid: %v", schemaid)

			// Create schema ID bucket
			schemaBucket, err := tx.CreateBucketIfNotExists([]byte(schemaid))
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("Wrote: %s", schemaid)

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

					digestBucket, err := valueBucket.CreateBucketIfNotExists([]byte(digest.Encoded()))
					if err != nil {
						log.Fatal(err)
					}
					log.Printf("Wrote Digest: %s", digest.String())

					// write namespace and digest of manifest
					repoBucket, err := digestBucket.CreateBucketIfNotExists([]byte(repo.Named().Name()))
					if err != nil {
						log.Fatal(err)
					}
					log.Printf("Wrote Repo: %s", repo.Named().Name())
					// Cursor produces debug output to server logs
					c := repoBucket.Cursor()
					for d, _ := c.First(); d != nil; d, _ = c.Next() {
						log.Printf("Wrote: /%v/%v/%v/%v", schemaid, keyname, v, digest.String(), repo.Named().Name())
					}

					if err != nil {
						log.Fatal(err)
					}
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
		if key == "" {
			key = "ai"
		}
		specAttributes[key] = append(specAttributes[key], jsonValue)
	}

	return specAttributes, nil
}
