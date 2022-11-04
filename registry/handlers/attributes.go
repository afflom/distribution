package handlers

import (
	"encoding/json"
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

	attribs := make(uor.AttributeSet)

	results := uor.ReadDB(attribs, ch.database)

	var manifest v1.Index

	resultm := make(map[digest.Digest]map[string][]byte)

	for _, result := range results {
		jsn, _ := json.Marshal(&result.AttribVal)
		attrib := make(map[string][]byte)
		attrib[result.AttribKey] = jsn
		resultm[result.Digest] = attrib
	}

	for _, result := range results {

		ann, _ := json.Marshal(resultm[result.Digest])
		uorAttrib := make(map[string]string)
		uorAttrib["uor.attributes"] = string(ann)
		desc := v1.Descriptor{
			MediaType:   v1.MediaTypeImageIndex,
			Size:        0,
			Digest:      result.Digest,
			Annotations: uorAttrib,
			Platform:    nil,
			URLs:        nil,
		}

		manifest.Manifests = append(manifest.Manifests, desc)

	}

	w.Header().Set("Content-Type", "application/json")

	enc := json.NewEncoder(w)
	if err := enc.Encode(attributesAPIResponse{}); err != nil {
		ch.Errors = append(ch.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}
