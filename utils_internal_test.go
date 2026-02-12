// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/AlphaOne1/dmorph"
)

func TestWrapError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err        error
		text       string
		wantErr    bool
		wantErrMsg string
	}{
		{ // 0
			text:       "wrap this",
			err:        errors.New("original error"),
			wantErr:    true,
			wantErrMsg: "wrap this: original error",
		},
		{ // 1
			text:       "wrap that: %w",
			err:        errors.New("original error"),
			wantErr:    true,
			wantErrMsg: "wrap that: %w: original error",
		},
		{ // 2
			text:    "wrap this",
			err:     nil,
			wantErr: false,
		},
		{ // 3
			text:       "",
			err:        errors.New("error case"),
			wantErr:    true,
			wantErrMsg: "error case",
		},
		{ // 4
			text:    "",
			err:     nil,
			wantErr: false,
		},
	}

	for testIndex, test := range tests {
		t.Run(fmt.Sprintf("WrapError-%d", testIndex), func(t *testing.T) {
			t.Parallel()

			got := dmorph.TwrapIfError(test.text, test.err)

			if (got != nil) != test.wantErr {
				t.Errorf(`got error "%v", but wanted "%v"`, got, test.wantErr)
			}

			if test.wantErr && got.Error() != test.wantErrMsg {
				t.Errorf(`got error "%v" but wanted "%v"`, got.Error(), test.wantErrMsg)
			}
		})
	}
}
