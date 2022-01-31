// Copyright (c) 2021 6 River Systems
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package filter

import (
	"fmt"
	"strconv"
	"unicode"
)

// Writer is a subset of strings.Builder
type Writer interface {
	WriteString(string) (int, error)
	WriteRune(rune) (int, error)
}

// AsFilter interface that will be implemented by filter
// structs with logic of stringifying them to filter expression
type AsFilter interface {
	AsFilter(Writer) error
}

// AsFilter implementation for Condition
func (e *Condition) AsFilter(w Writer) (err error) {
	if err = e.Term.AsFilter(w); err != nil {
		return
	}
	switch {
	case e.And != nil:
		return appendTerms(w, opAND, e.And)
	case e.Or != nil:
		return appendTerms(w, opOR, e.Or)
	default:
		return
	}
}

func appendTerms(w Writer, op booleanOperator, terms []*term) error {
	if len(terms) == 0 {
		return fmt.Errorf("filter: unpopulated %s sequence", op)
	}
	for _, ee := range terms {
		if _, err := w.WriteRune(' '); err != nil {
			return err
		}
		if _, err := w.WriteString(string(op)); err != nil {
			return err
		}
		if _, err := w.WriteRune(' '); err != nil {
			return err
		}
		if err := ee.AsFilter(w); err != nil {
			return fmt.Errorf("filter: error in %s sequence Term: %w", op, err)
		}
	}
	return nil
}

// AsFilter implementation for Term
func (e *term) AsFilter(w Writer) error {
	if e.Not {
		if _, err := w.WriteString("NOT "); err != nil {
			return err
		}
	}
	switch {
	case e.Basic != nil:
		if err := e.Basic.AsFilter(w); err != nil {
			return err
		}
	case e.Sub != nil:
		if _, err := w.WriteRune('('); err != nil {
			return err
		}
		if err := e.Sub.AsFilter(w); err != nil {
			return err
		}
		if _, err := w.WriteRune(')'); err != nil {
			return err
		}
	default:
		return ErrUnpopulatedTerm
	}
	return nil
}

// AsFilter implementation for BasicExpression
func (e *basicExpression) AsFilter(w Writer) error {
	switch {
	case e.Has != nil:
		return e.Has.AsFilter(w)
	case e.Value != nil:
		return e.Value.AsFilter(w)
	case e.Predicate != nil:
		return e.Predicate.AsFilter(w)
	default:
		return ErrUnpopulatedBasicExpression
	}
}

// AsFilter implementation for HasAttribute
func (e *hasAttribute) AsFilter(w Writer) error {
	if _, err := w.WriteString("attributes:"); err != nil {
		return err
	}
	if _, err := w.WriteString(formatAttrName(e.Name)); err != nil {
		return err
	}
	return nil
}

// AsFilter implementation for HasAttributeValue
func (e *hasAttributeValue) AsFilter(w Writer) error {
	if _, err := w.WriteString("attributes."); err != nil {
		return err
	}
	if _, err := w.WriteString(formatAttrName(e.Name)); err != nil {
		return err
	}
	if _, err := w.WriteString(string(e.Op)); err != nil {
		return err
	}
	if _, err := w.WriteString(strconv.Quote(e.Value)); err != nil {
		return err
	}
	return nil
}

// AsFilter implementation for HasAttributePredicate
func (e *hasAttributePredicate) AsFilter(w Writer) error {
	if _, err := w.WriteString(string(e.Predicate)); err != nil {
		return err
	}
	if _, err := w.WriteString("(attributes."); err != nil {
		return err
	}
	if _, err := w.WriteString(formatAttrName(e.Name)); err != nil {
		return err
	}
	if _, err := w.WriteRune(','); err != nil {
		return err
	}
	if _, err := w.WriteString(strconv.Quote(e.Value)); err != nil {
		return err
	}
	if _, err := w.WriteRune(')'); err != nil {
		return err
	}
	return nil
}

// Add Identation and quotes for attribute name
func formatAttrName(name string) string {
	isIdent := true
	for i, ch := range name {
		if !(ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch) && i > 0) {
			isIdent = false
			break
		}
	}
	if isIdent {
		return name
	}
	return strconv.Quote(name)
}
