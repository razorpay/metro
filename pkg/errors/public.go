package errors

import "github.com/razorpay/metro/pkg/errorresponse"

type IPublic interface {
	error
	GetCode() string
	GetField() string
	GetSource() string
	GetStep() string
	GetReason() string
	GetMetadata() map[string]string
}

type Public struct {
	Code        string
	Description string
	Field       string
	Source      string
	Step        string
	Reason      string
	Metadata    map[string]string
}

// newPublicFromCode creates a Public error from internal code
// this will lookup the error from the map provided
func newPublicFromCode(code string) *Public {
	var (
		meta map[string]string
		err  = mapping.Get(code)
	)

	if len(err.GetMetadata()) > 0 {
		meta = err.GetMetadata()
	} else {
		meta = make(map[string]string)
	}

	return &Public{
		Code:        err.GetCode(),
		Description: err.Error(),
		Field:       err.GetField(),
		Source:      err.GetSource(),
		Step:        err.GetStep(),
		Reason:      err.GetReason(),
		Metadata:    meta,
	}
}

// Error returns the error as string
func (p *Public) Error() string {
	return p.Description
}

// GetCode returns the error code
func (p *Public) GetCode() string {
	return p.Code
}

func (p *Public) GetField() string {
	return p.Field
}

func (p *Public) GetSource() string {
	return p.Source
}

// Step returns step when the error occurred
func (p *Public) GetStep() string {
	return p.Step
}

// Reason returns the error reason
func (p *Public) GetReason() string {
	return p.Reason
}

// Metadata returns the meta data of error
func (p *Public) GetMetadata() map[string]string {
	return p.Metadata
}

func (p *Public) toErrorResponse() errorresponse.Public {
	return errorresponse.Public{
		Code:        p.GetCode(),
		Description: p.Error(),
		Step:        errorresponse.String(p.GetStep()),
		Reason:      errorresponse.String(p.GetReason()),
		Field:       errorresponse.String(p.GetField()),
		Source:      errorresponse.String(p.GetSource()),
		Metadata:    p.GetMetadata(),
	}
}

// withCode updates the code if provided data is not empty
func (p *Public) withCode(code string) *Public {
	if code != "" {
		p.Code = code
	}

	return p
}

// withDescription updates the description if provided data is not empty
func (p *Public) withDescription(description string) *Public {
	if description != "" {
		p.Description = description
	}

	return p
}

// withSource updates the source if provided data is not empty
func (p *Public) withSource(source string) *Public {
	if source != "" {
		p.Source = source
	}

	return p
}

// withStep updates the step if provided data is not empty
func (p *Public) withStep(step string) *Public {
	if step != "" {
		p.Step = step
	}

	return p
}

// withReason updates the reason if provided data is not empty
func (p *Public) withReason(reason string) *Public {
	if reason != "" {
		p.Reason = reason
	}

	return p
}

// withField updates the field if provided data is not empty
func (p *Public) withField(field string) *Public {
	if field != "" {
		p.Field = field
	}

	return p
}

// withMetadata appends given meta data with Public meta data
func (p *Public) withMetadata(fields map[string]string) *Public {
	for key, val := range fields {
		p.Metadata[key] = val
	}

	return p
}
