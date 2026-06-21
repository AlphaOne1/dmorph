// SPDX-FileCopyrightText: 2026 The DMorph contributors.
// SPDX-License-Identifier: MPL-2.0

package dmorph_test

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
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

func TestSemVerSortPredicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   []string
		want []string
	}{
		{
			in:   []string{"v1.1.0", "v1.2.0"},
			want: []string{"v1.1.0", "v1.2.0"},
		},
		{
			in:   []string{"v1.2.0", "v1.1.0"},
			want: []string{"v1.1.0", "v1.2.0"},
		},
		{
			in:   []string{"v2.2.0", "v1.1.0"},
			want: []string{"v1.1.0", "v2.2.0"},
		},
		{
			in:   []string{"v1.1.1", "v1.1.0"},
			want: []string{"v1.1.0", "v1.1.1"},
		},
	}

	for testIndex, test := range tests {
		t.Run(fmt.Sprintf("SemVerSortPredicate-%d", testIndex), func(t *testing.T) {
			t.Parallel()

			slices.SortFunc(test.in, dmorph.TsemVerPrefixSortPredicate)

			if !reflect.DeepEqual(test.in, test.want) {
				t.Errorf("got %v, but wanted %v", test.in, test.want)
			}
		})
	}
}
