// SPDX-FileCopyrightText: 2026 The Dmorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// wrapIfError wraps an existing error with the provided text.
// It returns nil if `err` is nil, or the original `err` if `text` is empty.
// Otherwise, it returns a new error with the format "text: original_error".
func wrapIfError(text string, err error) error {
	if err == nil {
		return nil
	}

	if text == "" {
		return err
	}

	return fmt.Errorf("%s: %w", text, err)
}

var semVerPrefixRex = regexp.MustCompile(`^v[0-9]+[._][0-9]+[._][0-9]+`)

func semVerPrefixSortPredicate(a, b string) int {
	sa := semVerPrefixRex.FindString(a)
	sb := semVerPrefixRex.FindString(b)

	// if there is a non-semver prefix, we deem them equal
	if sa == "" || sb == "" {
		// we may think about a panic here, as this case _never_ should happen
		return 0
	}

	va := strings.Split(strings.ReplaceAll(sa[1:], "_", "."), ".")
	vb := strings.Split(strings.ReplaceAll(sb[1:], "_", "."), ".")

	for i := 0; i < len(va) && i < len(vb); i++ {
		ia, _ := strconv.Atoi(va[i])
		ib, _ := strconv.Atoi(vb[i])

		switch {
		case ia < ib:
			return -1
		case ia > ib:
			return 1
		}
	}

	return 0
}

func alphabeticalSortPredicate(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
