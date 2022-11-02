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
	Digest    string          `json:"digest"`
}

// ReadDB returns a ResulSet from an attribute Query of boltdb
func ReadDB(attribs AttributeSet, db *bolt.DB) ResultSet {

	var resultSet []Result

	log.Printf("Attribs: %s", attribs)
	for k, v := range attribs {
		log.Printf("Key: %s, Val: %s", k, v)
		if err := db.View(func(tx *bolt.Tx) error {
			log.Printf("Transaction Started")
			ak := tx.Bucket([]byte(k))
			for _, av := range v {
				vb := ak.Bucket([]byte(av))
				c := vb.Cursor()
				for r, d := c.First(); d != nil; d, r = c.Next() {
					fmt.Printf("A %s is %s.\n", d, r)
					result := Result{
						AttribKey: k,
						AttribVal: av,
						Namespace: string(r),
						Digest:    string(d),
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

	// todo(afflom): handle manifests with the other manifest handling logic
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

	for _, d := range man.Layers {
		for k, v := range d.Annotations {
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

	for k, v := range attribs {

		if err := db.Update(func(tx *bolt.Tx) error {

			kb, err := tx.CreateBucketIfNotExists([]byte(k))
			log.Printf("Top Level Bucket: %s", k)

			if err != nil {
				log.Fatal(err)
			}
			for _, av := range v {
				vb, err := kb.CreateBucketIfNotExists([]byte(av))
				log.Printf("Intermediary Bucket: %s", av)

				if err != nil {
					log.Fatal(err)
				}
				err = vb.Put([]byte(repo.Named().Name()), []byte(digest))
				log.Printf("Writing %s as %s", repo.Named().Name(), digest)
				c := vb.Cursor()
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

func convertAnnotationsToAttributes(annotations string) (map[string][]json.RawMessage, error) {
	specAttributes := map[string][]json.RawMessage{}

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
