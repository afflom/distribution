package handlers

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/distribution/distribution/v3/registry/api/errcode"
	"github.com/distribution/distribution/v3/uor"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func attributesDispatcher(ctx *Context, r *http.Request) http.Handler {
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

func (ch *attributesHandler) GetAttributes(w http.ResponseWriter, r *http.Request) {

	// Receive the query
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatalf("failed to get body: %v", err)
	}

	// Change the query back into an attribute set
	var attribs map[string]interface{}
	err = json.Unmarshal(body, &attribs)
	if err != nil {
		log.Fatal(err)
	}

	attributeMap := map[string]json.RawMessage{}
	for key, value := range attribs {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			log.Fatal(err)
		}
		attributeMap[key] = jsonValue
	}

	// Query the database

	results := uor.ReadDB(attributeMap, ch.database)

	var manifest v1.Index

	resultm := make(map[digest.Digest]map[string][]byte)

	// Deduplicate the database response
	for _, result := range results {
		jsn, _ := json.Marshal(&result.AttribVal)
		attrib := make(map[string][]byte)
		attrib[result.AttribKey] = jsn
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

	// Return the response to the caller

	w.Header().Set("Content-Type", "application/json")

	enc := json.NewEncoder(w)
	if err := enc.Encode(attributesAPIResponse{manifest}); err != nil {
		ch.Errors = append(ch.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}
