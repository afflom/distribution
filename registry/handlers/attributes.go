package handlers

import (
	"encoding/json"
	"log"
	"net/http"

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

	// Change the query back into an attribute set
	attributes := values.Get("query")
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

	results, err := uor.ReadDB(attributeMap, ah.database)
	if err != nil {
		log.Printf("error reading db: %v", err)
	}

	var manifest v1.Index

	resultm := make(map[digest.Digest]map[string][][]byte)

	// Deduplicate the database response
	for i, result := range results {

		jsn, _ := json.Marshal(&result.AttribVal)
		attrib := make(map[string][][]byte)
		log.Printf("deduplicating result %v", i)
		attrib[result.AttribKey] = append(attrib[result.AttribKey], jsn)
		resultm[result.Digest] = attrib
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
