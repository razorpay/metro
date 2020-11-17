package logger

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// An entry is the final or intermediate log entry. It contains all
// the fields passed with WithField{,s}. It's finally logged when Trace, Debug,
// Info, Warn, Error, Fatal or Panic is called on it. These objects can be
// reused and passed around as much as you wish to avoid field duplication.
type Entry struct {
	Logger *ZapLogger

	// Contains all the fields set by the user.
	Data map[string]interface{}

	// Level the log entry was logged at: Trace, Debug, Info, Warn, Error, Fatal or Panic
	// This field will be set on entry firing and the value will be equal to the one in Logger struct field.
	Level string

	// Message passed to Trace, Debug, Info, Warn, Error, Fatal or Panic
	Message string

	// err may contain a field formatting error
	err error
}

func NewEntry(logger *ZapLogger) *Entry {
	return &Entry{
		Logger: logger,
		// This is just an arbitrary assignment and not scientific to give it some room.
		Data: make(map[string]interface{}, 10),
	}
}

// Add an error as single field (using the key defined in ErrorKey) to the Entry.
func (entry *Entry) WithError(err error) *Entry {
	return entry.WithField(ErrorKey, err.Error())
}

// Add a single field to the Entry.
func (entry *Entry) WithField(key string, value interface{}) *Entry {
	return entry.WithFields(map[string]interface{}{key: value})
}

// Add a map of fields to the Entry.
func (entry *Entry) WithFields(fields map[string]interface{}) *Entry {
	data := make(map[string]interface{}, len(entry.Data)+len(fields))
	for k, v := range entry.Data {
		data[k] = v
	}
	fieldErr := entry.err
	for k, v := range fields {
		isErrField := false
		if t := reflect.TypeOf(v); t != nil {
			switch t.Kind() {
			case reflect.Func:
				isErrField = true
			case reflect.Ptr:
				isErrField = t.Elem().Kind() == reflect.Func
			}
		}
		if isErrField {
			tmp := fmt.Sprintf("can not add field %q", k)
			if fieldErr != nil {
				fieldErr = errors.New(entry.err.Error() + ", " + tmp)
			} else {
				fieldErr = errors.New(tmp)
			}
		} else {
			data[k] = v
		}
	}
	if entry.err != nil {
		entry.Data[ErrorKey] = fieldErr
	}
	return &Entry{Logger: entry.Logger, Data: data, err: entry.err}
}

func (entry *Entry) WithContext(ctx context.Context, ctxFields []string) *Entry {
	e := entry
	if ctx != nil && len(ctxFields) > 0 {
		for _, field := range ctxFields {
			val := ctx.Value(field)
			e = e.WithField(field, val)

		}
	}
	return e
}

// Debug logs a message with some additional context. `args`
// mentioned here as a key value map of extra context logging
// specific to this log message
func (entry *Entry) Debugw(format string, args map[string]interface{}) {
	entry.Logger.mu.Lock()
	defer entry.Logger.mu.Unlock()
	if len(entry.Data) != 0 {
		if args == nil {
			entry.Logger.sugaredLogger.Debugw(format, DefaultFieldConstant, entry.Data)
		} else {
			entry.Logger.sugaredLogger.Debugw(format, DefaultFieldConstant, entry.Data, config.ContextString, args)
		}
	} else {
		if args == nil {
			entry.Logger.sugaredLogger.Debugw(format)
		} else {
			entry.Logger.sugaredLogger.Debugw(format, config.ContextString, args)
		}

	}
}

// Debug logs a message as is. Default context is added here(if it exists)
func (entry *Entry) Debug(format string) {
	entry.Debugw(format, nil)
}

// Info logs a message with some additional context. `args`
// mentioned here as a key value map of extra context logging
// specific to this log message
func (entry *Entry) Infow(format string, args map[string]interface{}) {
	entry.Logger.mu.Lock()
	defer entry.Logger.mu.Unlock()
	if len(entry.Data) != 0 {
		if args == nil {
			entry.Logger.sugaredLogger.Infow(format, DefaultFieldConstant, entry.Data)
		} else {
			entry.Logger.sugaredLogger.Infow(format, DefaultFieldConstant, entry.Data, config.ContextString, args)
		}
	} else {
		if args == nil {
			entry.Logger.sugaredLogger.Infow(format)
		} else {
			entry.Logger.sugaredLogger.Infow(format, config.ContextString, args)
		}

	}
}

// Info logs a message as is. Default context is added here(if it exists)
func (entry *Entry) Info(format string) {
	entry.Infow(format, nil)
}

// Warn logs a message with some additional context. `args`
// mentioned here as a key value map of extra context logging
// specific to this log message
func (entry *Entry) Warnw(format string, args map[string]interface{}) {
	entry.Logger.mu.Lock()
	defer entry.Logger.mu.Unlock()
	if len(entry.Data) != 0 {
		if args == nil {
			entry.Logger.sugaredLogger.Warnw(format, DefaultFieldConstant, entry.Data)
		} else {
			entry.Logger.sugaredLogger.Warnw(format, DefaultFieldConstant, entry.Data, config.ContextString, args)
		}
	} else {
		if args == nil {
			entry.Logger.sugaredLogger.Warnw(format)
		} else {
			entry.Logger.sugaredLogger.Warnw(format, config.ContextString, args)
		}
	}
}

// Warn logs a message as is. Default context is added here(if it exists)
func (entry *Entry) Warn(format string) {
	entry.Warnw(format, nil)
}

// Warn logs a message with some additional context. `args`
// mentioned here as a key value map of extra context logging
// specific to this log message
func (entry *Entry) Errorw(format string, args map[string]interface{}) {
	entry.Logger.mu.Lock()
	defer entry.Logger.mu.Unlock()
	if len(entry.Data) != 0 {
		if args == nil {
			entry.Logger.sugaredLogger.Errorw(format, DefaultFieldConstant, entry.Data)
		} else {
			entry.Logger.sugaredLogger.Errorw(format, DefaultFieldConstant, entry.Data, config.ContextString, args)
		}
	} else {
		if args == nil {
			entry.Logger.sugaredLogger.Errorw(format)
		} else {
			entry.Logger.sugaredLogger.Errorw(format, config.ContextString, args)
		}
	}
}

// Error logs a message as is. Default context is added here(if it exists)
func (entry *Entry) Error(format string) {
	entry.Errorw(format, nil)
}

// Fatal logs a message with some additional context. `args`
// mentioned here as a key value map of extra context logging
// specific to this log message
func (entry *Entry) Fatalw(format string, args map[string]interface{}) {
	entry.Logger.mu.Lock()
	defer entry.Logger.mu.Unlock()
	if len(entry.Data) != 0 {
		if args == nil {
			entry.Logger.sugaredLogger.Fatalw(format, DefaultFieldConstant, entry.Data)
		} else {
			entry.Logger.sugaredLogger.Fatalw(format, DefaultFieldConstant, entry.Data, config.ContextString, args)
		}
	} else {
		if args == nil {
			entry.Logger.sugaredLogger.Fatalw(format)
		} else {
			entry.Logger.sugaredLogger.Fatalw(format, config.ContextString, args)
		}
	}
}

// Warn logs a message as is. Default context is added here(if it exists)
func (entry *Entry) Fatal(format string) {
	entry.Fatalw(format, nil)
}

// Panic logs a message with some additional context. `args`
// mentioned here as a key value map of extra context logging
// specific to this log message
func (entry *Entry) Panicw(format string, args map[string]interface{}) {
	entry.Logger.mu.Lock()
	defer entry.Logger.mu.Unlock()
	if len(entry.Data) != 0 {
		if args == nil {
			entry.Logger.sugaredLogger.Panicw(format, DefaultFieldConstant, entry.Data)
		} else {
			entry.Logger.sugaredLogger.Panicw(format, DefaultFieldConstant, entry.Data, config.ContextString, args)
		}
	} else {
		if args == nil {
			entry.Logger.sugaredLogger.Panicw(format)
		} else {
			entry.Logger.sugaredLogger.Panicw(format, config.ContextString, args)
		}
	}
}

// Panic logs a message as is. Default context is added here(if it exists)
func (entry *Entry) Panic(format string) {
	entry.Panicw(format, nil)
}
