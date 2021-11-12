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

// Filter Expressions will be mapped to the below defined
// Condition struct according to the logic provided using GO Struct Tags
// Example:
// "attributes:'language_tag' AND (attributes.domain = 'net' OR attributes.domain = 'org')"
// "attributes:'language_tag' AND NOT attributes.size = 'XL' AND hasPrefix(attributes.domain, 'co')"

// Condition contains term or multiple terms with 'AND' or 'OR' logical operator
type Condition struct {
	Term *term   `parser:"@@" json:"term,omitempty"`
	And  []*term `parser:"( (\"AND\" @@ )+" json:"and,omitempty"`
	Or   []*term `parser:"| (\"OR\" @@)+ )?" json:"or,omitempty"`
}

// term can be a Basic Expression or a Sub-Condition. It can also have 'NOT' boolean operator
type term struct {
	Not   bool             `parser:"@(\"NOT\"|\"-\")?" json:"NOT,omitempty"`
	Basic *basicExpression `parser:"( @@" json:"basic,omitempty"`
	Sub   *Condition       `parser:"| \"(\" @@ \")\" )" json:"sub,omitempty"`
}

// Basic Expression can be of any of the below types:
// 1. HasAttribute: If the given attribute is present
// 2. HasAttributeValue: If the given attribute is present with a give value
// 3. HasAttributePredicate: If the given attribute is present with the given predicate
type basicExpression struct {
	Has       *hasAttribute          `parser:"@@" json:"hasAttribute,omitempty"`
	Value     *hasAttributeValue     `parser:"| @@" json:"hasAttributeValue,omitempty"`
	Predicate *hasAttributePredicate `parser:"| @@" json:"hasAttributePredicate,omitempty"`
}

type hasAttribute struct {
	Name string `parser:"\"attributes\" \":\" @(Ident|String)" json:"name,omitempty"`
}

type hasAttributeValue struct {
	Name  string            `parser:"\"attributes\" \".\" @(Ident|String)" json:"name,omitempty"`
	Op    attributeOperator `parser:"@(\"=\" | \"!\" \"=\")" json:"operator,omitempty"`
	Value string            `parser:"@String" json:"value,omitempty"`
}

// hasAttributePredicate ..
// Supported Predicates : prefix
type hasAttributePredicate struct {
	Predicate attributePredicate `parser:"@(\"hasPrefix\")\"(\"" json:"predicate,omitempty"`
	Name      string             `parser:"\"attributes\" \".\" @(Ident|String) \",\"" json:"name,omitempty"`
	Value     string             `parser:"@String \")\"" json:"value,omitempty"`
}

type attributeOperator string

const (
	opEqual    attributeOperator = "="
	opNotEqual attributeOperator = "!="
)

type attributePredicate string

const (
	predicateHasPrefix attributePredicate = "hasPrefix"
)

type booleanOperator string

const (
	opAND booleanOperator = "AND"
	opOR  booleanOperator = "OR"
)
