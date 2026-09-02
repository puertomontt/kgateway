package listener

import (
	"fmt"
	"strings"

	"github.com/kgateway-dev/kgateway/v2/pkg/krtcollections"
)

// FormatRefErrorMessage renders err as a status condition message describing failed
// references.
//
// A bare *krtcollections.NotFoundError is reworded with ResourceNotFoundMessageTemplate, so
// that a missing reference reads the same way wherever it is reported, and its Hint is kept.
// Any other error keeps the text its producer wrote — including an error that wraps a
// NotFoundError, whose own context is more specific than the template.
//
// errors.Join trees are flattened and every leaf is rendered, joined with "; ", so a listener
// with several bad certificateRefs reports all of them instead of collapsing to whichever one
// happens to be not found. Joining with "; " rather than the "\n" that errors.Join uses keeps
// the message readable as a single condition line.
func FormatRefErrorMessage(err error) string {
	return strings.Join(appendRefErrorMessages(nil, err), "; ")
}

func appendRefErrorMessages(msgs []string, err error) []string {
	if err == nil {
		return msgs
	}

	// Inspecting this error's own structure, not matching a target: errors.As cannot
	// enumerate the leaves of a join.
	//nolint:errorlint // see above
	if aggregate, ok := err.(interface{ Unwrap() []error }); ok {
		leaves := aggregate.Unwrap()
		if isJoin(err, leaves) {
			for _, leaf := range leaves {
				msgs = appendRefErrorMessages(msgs, leaf)
			}
			return msgs
		}
	}

	// Deliberately the outermost error only. errors.As would reach a NotFoundError through a
	// wrapper and reword it, dropping the context the wrapper added.
	//nolint:errorlint // see above
	if notFound, ok := err.(*krtcollections.NotFoundError); ok {
		return append(msgs, notFoundRefMessage(notFound))
	}

	return append(msgs, err.Error())
}

// isJoin reports whether err is an errors.Join of leaves — an aggregator that adds no text of
// its own, and so can be taken apart — rather than a fmt.Errorf that wrapped several errors
// with %w, whose format string carries the message and must be kept. Both expose
// Unwrap() []error and are otherwise indistinguishable; what separates them is that
// errors.Join renders as exactly its children joined by newlines.
func isJoin(err error, leaves []error) bool {
	texts := make([]string, 0, len(leaves))
	for _, leaf := range leaves {
		texts = append(texts, leaf.Error())
	}
	return err.Error() == strings.Join(texts, "\n")
}

// notFoundRefMessage renders a NotFoundError with ResourceNotFoundMessageTemplate, followed by
// the error's Hint if it has one.
func notFoundRefMessage(notFound *krtcollections.NotFoundError) string {
	resourceType := notFound.NotFoundObj.Kind
	if resourceType == "" {
		resourceType = "Resource"
	}

	msg := fmt.Sprintf(
		ResourceNotFoundMessageTemplate,
		resourceType,
		notFound.NotFoundObj.Namespace,
		notFound.NotFoundObj.Name,
	)
	if notFound.Hint != "" {
		msg += " " + notFound.Hint
	}
	return msg
}
