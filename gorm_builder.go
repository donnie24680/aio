package o

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"gorm.io/gorm"
)

// Where condition query type, using [][]any format
type Where [][]any

func W(str string, arg any, w ...any) Where {
	args := append([]any{str}, arg)
	args = append(args, w...)
	return [][]any{args}
}

// Query returns GORM query builder, supports all GORM native methods
func Query(table string, args ...Where) *gorm.DB {
	query := GetDBDefault().Table(table)
	return applyGormWhere(query, args...)
}

// QueryTx query in transaction, returns GORM query builder
func QueryTx(tx *gorm.DB, table string, args ...Where) *gorm.DB {
	if tx == nil {
		return nil
	}
	query := tx.Table(table)
	return applyGormWhere(query, args...)
}

// Find query all records
func Find(v any, table string, args ...Where) error {
	return Query(table, args...).Find(v).Error
}

// First query first record
func First(v any, table string, args ...Where) error {
	return Query(table, args...).First(v).Error
}

// Last query last record
func Last(v any, table string, args ...Where) error {
	return Query(table, args...).Last(v).Error
}

// Take query one record (order not guaranteed)
func Take(v any, table string, args ...Where) error {
	return Query(table, args...).Take(v).Error
}

// Count count record quantity
func Count(table string, args ...Where) (int64, error) {
	var count int64
	err := Query(table, args...).Count(&count).Error
	return count, err
}

// v supports string and number
func Sum(v any, table string, field string, args ...Where) error {
	err := Query(table, args...).Select("SUM(" + field + ")").Scan(v).Error
	return err
}

// Pluck query single field value
func Pluck(v any, table string, field string, args ...Where) error {
	return Query(table, args...).Pluck(field, v).Error
}

// Scan scan query results to specified struct
func Scan(v any, table string, args ...Where) error {
	return Query(table, args...).Scan(v).Error
}

// Paginate pagination query
func Paginate(v any, table string, offset, limit int, args ...Where) (int64, error) {
	query := Query(table, args...)

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return 0, err
	}

	err := query.Offset(offset).Limit(limit).Find(v).Error
	return total, err
}

// Insert insert single record
func Insert(table string, data any) error {
	return GetDBDefault().Table(table).Create(data).Error
}

// InsertStruct insert single record, automatically handle CreateTime and UpdateTime fields
func InsertStruct(table string, data any) error {
	// Use reflection to automatically set time fields
	if err := setTimeFields(data, true, true); err != nil {
		return err
	}
	return GetDBDefault().Table(table).Create(data).Error
}

// InsertTx insert single record in transaction
func InsertTx(tx *gorm.DB, table string, data any) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	return tx.Table(table).Create(data).Error
}

// InsertStructTx insert single record in transaction, automatically handle CreateTime and UpdateTime fields
func InsertStructTx(tx *gorm.DB, table string, data any) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	// Use reflection to automatically set time fields
	if err := setTimeFields(data, true, true); err != nil {
		return err
	}
	return tx.Table(table).Create(data).Error
}

// InsertBatch batch insert records
func InsertBatch(table string, data []any) error {
	return GetDBDefault().Table(table).CreateInBatches(data, 100).Error
}

// InsertBatchTx batch insert records in transaction
func InsertBatchTx(tx *gorm.DB, table string, data []any) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	return tx.Table(table).CreateInBatches(data, 100).Error
}

// Update update records
func Update(table string, data any, args ...Where) error {
	if len(args) == 0 || args == nil {
		return fmt.Errorf("update: invalid condition")
	}
	query := Query(table, args...)
	return query.Updates(data).Error
}

// UpdateTx update records in transaction
func UpdateTx(tx *gorm.DB, table string, data any, args ...Where) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	if len(args) == 0 || args == nil {
		return fmt.Errorf("update: invalid condition")
	}
	query := QueryTx(tx, table, args...)
	return query.Updates(data).Error
}

// Delete delete records
func Delete(table string, args ...Where) error {
	if len(args) == 0 || args == nil {
		return fmt.Errorf("delete: invalid condition")
	}
	query := Query(table, args...)
	return query.Delete(&struct{}{}).Error
}

// DeleteTx delete records in transaction
func DeleteTx(tx *gorm.DB, table string, args ...Where) error {
	if tx == nil {
		return fmt.Errorf("delete: invalid condition")
	}
	if len(args) == 0 || args == nil {
		return fmt.Errorf("delete: invalid condition")
	}
	query := QueryTx(tx, table, args...)
	return query.Delete(&struct{}{}).Error
}

// Exists check if record exists
func Exists(table string, args ...Where) (bool, error) {
	query := Query(table, args...)
	var count int64
	if err := query.Limit(1).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// applyGormWhere apply WHERE conditions to GORM query - use parameterized queries to prevent SQL injection
func applyGormWhere(query *gorm.DB, args ...Where) *gorm.DB {
	for _, w := range args {
		for _, arg := range w {
			switch len(arg) {
			case 2:
				// Two parameters: field and value
				field := arg[0].(string)
				query = query.Where(field+" = ?", arg[1])
			case 3:
				// Three parameters: field, operator, value
				field := arg[0].(string)
				operator, ok := arg[1].(string)
				if !ok {
					continue
				}
				value := arg[2]
				operator = strings.ToLower(operator)

				switch operator {
				case "=", ">", "<", ">=", "<=", "!=", "<>":
					query = query.Where(field+" "+operator+" ?", value)
				case "like", "not like":
					query = query.Where(field+" "+operator+" ?", value)
				case "in", "not in":
					query = query.Where(field+" "+operator+" (?)", value)
				case "between", "not between":
					if slice, ok := value.([]interface{}); ok && len(slice) == 2 {
						query = query.Where(field+" "+operator+" ? AND ?", slice[0], slice[1])
					}
				}
			default:
				sqlStr := arg[0].(string)
				params := arg[1:]
				query = query.Where(sqlStr, params...)
			}
		}
	}
	return query
}

// setTimeFields automatically set time fields in struct through reflection
func setTimeFields(data any, setCreateTime, setUpdateTime bool) error {
	if data == nil {
		return nil
	}

	// Get reflection value
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// If not a struct, return directly
	if v.Kind() != reflect.Struct {
		return nil
	}

	now := time.Now()

	// Iterate through struct fields
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := v.Type().Field(i)

		// Check if field can be set
		if !field.CanSet() {
			continue
		}

		// Check if field type is time.Time
		if field.Type() == reflect.TypeOf(time.Time{}) {
			fieldName := fieldType.Name

			// Automatically set time based on field name
			switch {
			case setCreateTime && (fieldName == "CreateTime" || fieldName == "CreatedAt"):
				field.Set(reflect.ValueOf(now))
			case setUpdateTime && (fieldName == "UpdateTime" || fieldName == "UpdatedAt"):
				field.Set(reflect.ValueOf(now))
			}
		}
	}

	return nil
}
