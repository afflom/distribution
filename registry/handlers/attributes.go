package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/distribution/distribution/v3/registry/api/errcode"
	"github.com/distribution/distribution/v3/uor"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func attributesDispatcher(ctx *Context, r *http.Request) http.Handler {
	log.Printf("Hit Attributes dispatcher!")

	attributesHandler := &attributesHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		"GET": http.HandlerFunc(attributesHandler.GetAttributes),
	}
}

type attributesHandler struct {
	*Context
}

type attributesAPIResponse struct {
	v1.Index
}

func (ah *attributesHandler) GetAttributes(w http.ResponseWriter, r *http.Request) {
	log.Printf("Hit Attributes endpoint!")

	// Receive the query
	values := r.URL.Query()

	log.Printf("values: %v", values)

	var manifest v1.Index
	resultm := make(map[digest.Digest]map[string]string)

	// Query by attributes
	if attributes := values.Get("attributes"); attributes != "" {
		var attribs map[string]interface{}
		err := json.Unmarshal([]byte(attributes), &attribs)

		if err != nil {
			ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}

		attributeMap := map[string]json.RawMessage{}
		for key, value := range attribs {
			jsonValue, err := json.Marshal(value)
			if err != nil {
				ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
				return
			}
			attributeMap[key] = jsonValue
		}

		// Query the database

		results, err := uor.AttributeQuery(attributeMap, ah.database)
		if err != nil {
			log.Printf("error reading db: %v", err)
		}

		// Deduplicate the database response
		for _, result := range results {

			//jsn, _ := json.Marshal(&result.AttribVal)
			//attrib := make(map[string][][]byte)
			//log.Printf("deduplicating result %v", i)
			//attrib[result.AttribKey] = append(attrib[result.AttribKey], jsn)
			attributes := make(map[string]string)
			//schema[result.Schema] = attrib
			resultm[result.Digest] = attributes
		}
	}

	// Query links
	if l := values.Get("links"); l != "" {

		log.Printf("links parameter hit: %v", l)

		links := strings.Split(l, ",")

		ln, _ := json.Marshal(links)
		log.Printf("resolving %v", string(ln))

		log.Println("query was split")
		resolvedLinks, err := uor.LinkQuery(links, ah.database)
		if err != nil {
			log.Printf("error reading db: %v", err)
		}

		for linker, target := range resolvedLinks {
			coreLink := make(map[string]string) // core-link/registryHint/NamespaceHints

			t, err := json.Marshal(target)
			if err != nil {
				log.Printf("error reading db: %v", err)
			}

			log.Printf("link target: %v", string(t))

			coreLink["core-link"] = string(t)

			resultm[linker] = coreLink
		}
	}

	// Resolve digests
	// Query by digest
	d := values.Get("digests")
	var digests []string
	if d != "" {
		digests = strings.Split(d, ",")
	}
	for k := range resultm {
		digests = append(digests, k.String())
		log.Printf("Adding digest: %v to digest query", k.String())

	}

	resolvedDigests, err := uor.DigestQuery(digests, ah.database)
	log.Printf("Resolved digest map: %v", resolvedDigests)

	if err != nil {
		log.Printf("error reading db: %v", err)
	}

	for _, target := range resolvedDigests {
		localNamespaces := make(map[string]string) // core-link/registryHint/NamespaceHints

		t, err := json.Marshal(target)
		if err != nil {
			log.Printf("error reading db: %v", err)
		}

		localNamespaces["LocalNamespaces"] = string(t)

		resultm[target.Target] = localNamespaces
	}

	// Write the index manifest with the resolved descriptors
	for digest, result := range resultm {

		ann, _ := json.Marshal(result)
		uorAttrib := make(map[string]string)
		uorAttrib["uor.attributes"] = string(ann)
		desc := v1.Descriptor{
			MediaType:   v1.MediaTypeImageIndex,
			Size:        0,
			Digest:      digest,
			Annotations: uorAttrib,
			Platform:    nil,
			URLs:        nil,
		}

		manifest.Manifests = append(manifest.Manifests, desc)

	}

	manifest.SchemaVersion = 2

	// Return the response to the caller

	w.Header().Set("Content-Type", "application/json")

	enc := json.NewEncoder(w)
	if err := enc.Encode(attributesAPIResponse{manifest}); err != nil {
		ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}
