package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	dcontext "github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/registry/api/errcode"
	"github.com/distribution/distribution/v3/uor"
)

const (
	AttributesParamKey = "attributes"
	DigestParamKey     = "digest"
	LinkParamKey       = "links"
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

func (ah *attributesHandler) GetAttributes(w http.ResponseWriter, r *http.Request) {
	logger := dcontext.GetLogger(ah)
	logger.Debug("ResolveAttributeQuery")
	// Receive the query
	values := r.URL.Query()

	var manifest v1.Index
	queryResults := make(map[digest.Digest]map[string]string)

	// Construct attributes query for database input
	if attributes := values.Get(AttributesParamKey); attributes != "" {
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

		// Query database for attributes
		results, err := uor.AttributeQuery(attributeMap, ah.database)
		if err != nil {
			ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}

		// Filter the query
		filter := make(map[digest.Digest]map[string]map[string][]json.RawMessage)
		unfilteredResults := make(map[digest.Digest]map[string]string)

		logger.Debugln("Results found, filtering...")

		for _, result := range results {
			schemaSet, ok := filter[result.Digest]
			if !ok {
				schemaSet = map[string]map[string][]json.RawMessage{}
			}
			attributePairs, ok := schemaSet[result.Schema]
			if !ok {
				attributePairs = map[string][]json.RawMessage{}
			}

			attributePairs[result.AttribKey] = append(attributePairs[result.AttribKey], result.AttribVal)
			schemaSet[result.Schema] = attributePairs
			filter[result.Digest] = schemaSet

			attributes := make(map[string]string)
			//schema[result.Schema] = attrib
			unfilteredResults[result.Digest] = attributes
		}

		// Transform user submitted attribute query into a filterable format.
		submittedAttributes := make(map[string]map[string]json.RawMessage)
		for schema, attr := range attributeMap {
			var jsonData map[string]interface{}
			if err := json.Unmarshal(attr, &jsonData); err != nil {
				ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
				continue
			}
			attrPair, ok := submittedAttributes[schema]
			if !ok {
				attrPair = map[string]json.RawMessage{}
			}
			for key, value := range jsonData {
				jsonValue, err := json.Marshal(value)
				if err != nil {
					ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
					continue
				}
				attrPair[key] = jsonValue
				submittedAttributes[schema] = attrPair
			}
		}

		var keep []digest.Digest
		for digest, filterAttr := range filter {
			logger.Infof("evaluating digest %s\n", digest)
			fullMatch := ah.matchAttributes(filterAttr, submittedAttributes)
			if fullMatch {
				logger.Infof("Digest %s is a full match\n", digest)
				keep = append(keep, digest)
			}
		}

		for _, d := range keep {
			queryResults[d] = unfilteredResults[d]
		}
	}
	// Query links
	if l := values.Get(LinkParamKey); l != "" {

		logger.Debugf("links parameter hit: %v", l)
		links := strings.Split(l, ",")

		ln, err := json.Marshal(links)
		if err != nil {
			ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}
		logger.Debugf("resolving %v", string(ln))

		logger.Debug("query was split")
		resolvedLinks, err := uor.LinkQuery(links, ah.database)
		if err != nil {
			ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}

		for linker, target := range resolvedLinks {
			coreLink := make(map[string]string) // core-link/registryHint/NamespaceHints
			t, err := json.Marshal(target)
			if err != nil {
				ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
				return
			}
			logger.Infof("link target: %v", string(t))
			coreLink["core-link"] = string(t)
			queryResults[linker] = coreLink
		}
	}

	// Resolve digests
	d := values.Get(DigestParamKey)
	var digests []string
	if d != "" {
		digests = strings.Split(d, ",")
	}
	for k := range queryResults {
		digests = append(digests, k.String())
		logger.Infof("Adding digest: %v to digest query", k.String())
	}

	resolvedDigests, err := uor.DigestQuery(digests, ah.database)
	if err != nil {
		ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}

	for _, target := range resolvedDigests {
		localNamespaces := make(map[string]string) // core-link/registryHint/NamespaceHints
		t, err := json.Marshal(target)
		if err != nil {
			ah.Errors = append(ah.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}
		localNamespaces["LocalNamespaces"] = string(t)
		queryResults[target.Target] = localNamespaces
	}

	// Write the index manifest with the resolved descriptors
	for digest, result := range queryResults {

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

// matchAttributes checks that the given input set contains all attributes in the querySet.
func (ah *attributesHandler) matchAttributes(inputSet map[string]map[string][]json.RawMessage, querySet map[string]map[string]json.RawMessage) bool {
	logger := dcontext.GetLogger(ah)
	// for each queried attribute
	for schema, attributeSet := range querySet {
		// If the schema does not exist in the input set
		// return false
		inputAttributeSet, found := inputSet[schema]
		if !found {
			logger.Infof("schema %s not found in input set", schema)
			return false
		}

		//
		for key, attrVal := range attributeSet {
			// If there is no key i
			inputValues, found := inputAttributeSet[key]
			if !found {
				logger.Infof("key %s not found in input set", key)
				return false
			}

			// If the attribute value is found
			var attrMatch bool
			for _, val := range inputValues {
				if string(val) == string(attrVal) {
					attrMatch = true
					break
					logger.Infof("pair %s=%s found!", key, string(val))
				}
			}
			if !attrMatch {
				logger.Infof("value %s not found in input set", string(attrVal))
				return false
			}
		}
	}
	return true
}
