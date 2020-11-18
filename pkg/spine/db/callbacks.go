package db

import (
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
)

func replaceGormCallbacks(db *DB) {
	// We are using registered callback as GORM callbacks cannot be de-registered.
	// It can only be replaced. In case of FirstOrCreate it always calls registered callback
	// In this case the callback written in models wont come into picture.
	db.instance.Callback().Update().Replace("gorm:update_time_stamp", updateTimestampOnUpdate)
	db.instance.Callback().Create().Replace("gorm:update_time_stamp", updateTimestampOnCreate)

	// GORM does not have any callback already registered for delete operation
	// So we are replacing new callback which will be triggered when delete operation happens
	// And will update deleted timestamp to the scope id the column defined the that scope
	db.instance.Callback().Delete().Replace("gorm:delete", updateTimestampOnDelete)
}

// updateTimestampOnCreate updates both createdAt and updatedAt using single ts instance
// else we can have different time in createdAt and updatedAt
func updateTimestampOnCreate(scope *gorm.Scope) {
	if createdAtField, ok := scope.FieldByName("CreatedAt"); ok {
		// If its blank then only we update it with current timestamp
		if createdAtField.IsBlank {
			_ = scope.SetColumn("CreatedAt", time.Now().Unix())
		}
	}

	updateTimestampOnUpdate(scope)
}

// updateTimestampOnUpdate updatedAt using single ts instance
func updateTimestampOnUpdate(scope *gorm.Scope) {
	if _, ok := scope.FieldByName("UpdatedAt"); ok {
		_ = scope.SetColumn("UpdatedAt", time.Now().Unix())
	}
}

// updateTimestampOnDelete updated current unix timestamp to updated_at column of the scope
func updateTimestampOnDelete(scope *gorm.Scope) {
	// Source: gorm.deleteCallback()
	// Change is using unix timestamp value instead of gorm.NowFunc().
	if !scope.HasError() {
		var extraOption string
		if str, ok := scope.Get("gorm:delete_option"); ok {
			extraOption = fmt.Sprint(str)
		}

		deletedAtField, hasDeletedAtField := scope.FieldByName("DeletedAt")

		if !scope.Search.Unscoped && hasDeletedAtField {
			ts := time.Now().Unix()

			scope.Raw(fmt.Sprintf(
				"UPDATE %v SET %v=%v%v%v",
				scope.QuotedTableName(),
				scope.Quote(deletedAtField.DBName),
				ts,
				addExtraSpaceIfExist(scope.CombinedConditionSql()),
				addExtraSpaceIfExist(extraOption),
			)).Exec()
		} else {
			scope.Raw(fmt.Sprintf(
				"DELETE FROM %v%v%v",
				scope.QuotedTableName(),
				addExtraSpaceIfExist(scope.CombinedConditionSql()),
				addExtraSpaceIfExist(extraOption),
			)).Exec()
		}
	}
}

func addExtraSpaceIfExist(str string) string {
	// Source: gorm.addExtraSpaceIfExist()
	if str != "" {
		return " " + str
	}
	return ""
}
