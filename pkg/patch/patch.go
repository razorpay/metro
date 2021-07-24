package patch

import (
	"context"
	"fmt"
	"reflect"

	"github.com/oleiade/reflections"
	"github.com/razorpay/metro/pkg/logger"
)

//StructPatch - Patches the attributes of struct `fromObj` into struct `toObj`
//The underlying types of both these objects must be the same
//fields define the fields that are to be copied
func StructPatch(ctx context.Context, fromObj, toObj interface{}, fields []string) error {
	typeA := reflect.TypeOf(fromObj)
	typeB := reflect.TypeOf(toObj)

	if typeA.Kind() != reflect.Ptr || typeA.Elem().Kind() != reflect.Struct {
		logger.Ctx(ctx).Errorw("patching allowed only for pointer to structs", "type", typeA)
		err := fmt.Errorf("invalid patching")
		return err
	}

	if typeA != typeB {
		logger.Ctx(ctx).Errorw("patching incompatible types", "typeA", typeA, "typeB", typeB)
		err := fmt.Errorf("invalid patching")
		return err
	}

	for _, field := range fields {
		patch, err := reflections.GetField(fromObj, field)
		if err != nil {
			return err
		}
		if err = reflections.SetField(toObj, field, patch); err != nil {
			return err
		}
	}

	return nil
}
