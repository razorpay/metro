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
	"errors"
	"fmt"
	"strings"
)

// Evaluator interface which all grammar components should implement
type Evaluator interface {
	Evaluate(attrs map[string]string) (bool, error)
}

// Evaluate implementation for Basic Expression
func (e *basicExpression) Evaluate(attrs map[string]string) (bool, error) {
	switch {
	case e.Has != nil:
		return e.Has.Evaluate(attrs)
	case e.Value != nil:
		return e.Value.Evaluate(attrs)
	case e.Predicate != nil:
		return e.Predicate.Evaluate(attrs)
	default:
		return false, errors.New("unpopulated BasicExpression")
	}
}

// Evaluate implementation for Condition
func (e *Condition) Evaluate(attrs map[string]string) (result bool, err error) {
	result, err = e.Term.Evaluate(attrs)
	if err != nil {
		return
	}
	switch {
	case e.And != nil:
		if result {
			result, err = andTerms(attrs, e.And)
		}
	case e.Or != nil:
		if !result {
			result, err = orTerms(attrs, e.Or)
		}
	}
	return
}

// Evaluate implementation for Term
func (e *term) Evaluate(attrs map[string]string) (result bool, err error) {
	switch {
	case e.Basic != nil:
		result, err = e.Basic.Evaluate(attrs)
	case e.Sub != nil:
		result, err = e.Sub.Evaluate(attrs)
	default:
		return false, errors.New("unpopulated Term")
	}
	result = result != e.Not // XOR
	return result, err
}

// Evaluate implementation for HasAttribute
func (e *hasAttribute) Evaluate(attrs map[string]string) (bool, error) {
	_, ok := attrs[e.Name]
	return ok, nil
}

// Evaluate implementation for HasAttributeValue
func (e *hasAttributeValue) Evaluate(attrs map[string]string) (bool, error) {
	v, ok := attrs[e.Name]
	if !ok {
		return false, nil
	}
	switch e.Op {
	case opEqual:
		return v == e.Value, nil
	case opNotEqual:
		return v != e.Value, nil
	default:
		return false, fmt.Errorf("invalid Op '%s'", e.Op)
	}
}

// Evaluate implementation for HasAttributePredicate
func (e *hasAttributePredicate) Evaluate(attrs map[string]string) (bool, error) {
	v, ok := attrs[e.Name]
	if !ok {
		return false, nil
	}
	switch e.Predicate {
	case predicateHasPrefix:
		return strings.HasPrefix(v, e.Value), nil
	default:
		return false, fmt.Errorf("invalid predicate '%s'", e.Predicate)
	}
}

func andTerms(attrs map[string]string, terms []*term) (result bool, err error) {
	if len(terms) == 0 {
		return false, errors.New("AND requires a non-empty term list")
	}
	for _, t := range terms {
		if result, err = t.Evaluate(attrs); err != nil || !result {
			return
		}
	}
	return
}

func orTerms(attrs map[string]string, terms []*term) (result bool, err error) {
	if len(terms) == 0 {
		return false, errors.New("OR requires a non-empty term list")
	}
	for _, t := range terms {
		if result, err = t.Evaluate(attrs); err != nil || result {
			return
		}
	}
	return
}
