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
	manifest.SchemaVersion = 2
	queryResults := make(map[digest.Digest][]v1.Descriptor)

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
		unfilteredResults := make(map[digest.Digest][]v1.Descriptor)

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

			desc := v1.Descriptor{
				MediaType: v1.MediaTypeImageManifest,
				Digest:    result.Digest,
			}

			unfilteredResults[result.Digest] = append(unfilteredResults[result.Digest], desc)
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
			queryResults[linker] = append(queryResults[linker], target...)
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
		queryResults[target.Digest] = append(queryResults[target.Digest], target)
	}

	// Write the index manifest with the resolved descriptors
	for _, result := range queryResults {
		manifest.Manifests = append(manifest.Manifests, result...)
	}

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
