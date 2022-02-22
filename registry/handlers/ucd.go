package handlers

import (
	"fmt"
	"net/http"

	"github.com/distribution/distribution/v3"
	dcontext "github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/registry/api/errcode"
	v2 "github.com/distribution/distribution/v3/registry/api/v2"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
)

// ucdDispatcher takes the request context and builds the
// appropriate handler for handling manifest requests.
func ucdDispatcher(ctx *Context, r *http.Request) http.Handler {
	manifestHandler := &manifestHandler{
		Context: ctx,
	}
	reference := getReference(ctx)
	dgst, err := digest.Parse(reference)
	if err != nil {
		// We just have a tag
		manifestHandler.Tag = reference
	} else {
		manifestHandler.Digest = dgst
	}

	mhandler := handlers.MethodHandler{
		"GET":  http.HandlerFunc(manifestHandler.GetUCD),
		"HEAD": http.HandlerFunc(manifestHandler.GetUCD),
	}

	return mhandler
}

// ucdHandler handles http operations on ucd manifests.
//type ucdHandler struct {
//	manifestHandler
//}

// GetUCD fetches the UCD blob from the storage backend, by referencing the UCD manifest, if it exists.
func (uh manifestHandler) GetUCD(w http.ResponseWriter, r *http.Request) {
	dcontext.GetLogger(uh).Debug("GetUCDManifest")
	manifests, err := uh.Repository.Manifests(uh)
	if err != nil {
		uh.Errors = append(uh.Errors, err)
		return
	}

	if uh.Tag != "" {
		tags := uh.Repository.Tags(uh)
		desc, err := tags.Get(uh, uh.Tag)
		if err != nil {
			if _, ok := err.(distribution.ErrTagUnknown); ok {
				uh.Errors = append(uh.Errors, v2.ErrorCodeManifestUnknown.WithDetail(err))
			} else {
				uh.Errors = append(uh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			}
			return
		}
		uh.Digest = desc.Digest
	}

	var options []distribution.ManifestServiceOption
	if uh.Tag != "" {
		options = append(options, distribution.WithTag(uh.Tag))
	}
	manifest, err := manifests.Get(uh, uh.Digest, options...)
	if err != nil {
		if _, ok := err.(distribution.ErrManifestUnknownRevision); ok {
			uh.Errors = append(uh.Errors, v2.ErrorCodeManifestUnknown.WithDetail(err))
		} else {
			uh.Errors = append(uh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		}
		return
	}
	// Get the digest and mediatype of the only blob in the manifest
	var blobDigest digest.Digest
	var blobMt string
	for _, b := range manifest.References() {
		blobDigest = b.Digest
		blobMt = b.MediaType
	}
	blobHandler := &blobHandler{
		Context: uh.Context,
		Digest:  blobDigest,
	}

	blobs := blobHandler.Repository.Blobs(blobHandler)
	desc, err := blobs.Stat(blobHandler, blobHandler.Digest)
	if err != nil {
		uh.Errors = append(uh.Errors, err)
		return
	}
	blen := desc.Size

	w.Header().Set("Content-Type", blobMt)
	w.Header().Set("Content-Length", fmt.Sprint(blen))
	blobHandler.GetBlob(w, r)

}

/*
// applyResourcePolicy checks whether the resource class matches what has
// been authorized and allowed by the policy configuration.
func (uh *manifestHandler) applyUCDResourcePolicy(manifest distribution.Manifest) error {
	allowedClasses := uh.App.Config.Policy.Repository.Classes
	if len(allowedClasses) == 0 {
		return nil
	}

	var class string
	switch m := manifest.(type) {
	case *schema1.SignedManifest:
		class = imageClass
	case *schema2.DeserializedManifest:
		switch m.Config.MediaType {
		case schema2.MediaTypeImageConfig:
			class = imageClass
		case schema2.MediaTypePluginConfig:
			class = "plugin"
		default:
			return errcode.ErrorCodeDenied.WithMessage("unknown manifest class for " + m.Config.MediaType)
		}
	case *ocischema.DeserializedManifest:
		switch m.Config.MediaType {
		case v1.MediaTypeImageConfig:
			class = imageClass
		default:
			return errcode.ErrorCodeDenied.WithMessage("unknown manifest class for " + m.Config.MediaType)
		}
	}

	if class == "" {
		return nil
	}

	// Check to see if class is allowed in registry
	var allowedClass bool
	for _, c := range allowedClasses {
		if class == c {
			allowedClass = true
			break
		}
	}
	if !allowedClass {
		return errcode.ErrorCodeDenied.WithMessage(fmt.Sprintf("registry does not allow %s manifest", class))
	}

	resources := auth.AuthorizedResources(uh)
	n := uh.Repository.Named().Name()

	var foundResource bool
	for _, r := range resources {
		if r.Name == n {
			if r.Class == "" {
				r.Class = imageClass
			}
			if r.Class == class {
				return nil
			}
			foundResource = true
		}
	}

	// resource was found but no matching class was found
	if foundResource {
		return errcode.ErrorCodeDenied.WithMessage(fmt.Sprintf("repository not authorized for %s manifest", class))
	}

	return nil

}
*/
