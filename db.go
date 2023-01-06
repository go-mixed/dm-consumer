package consumer

import (
	"bytes"
	"encoding/json"
	"gopkg.in/go-mixed/dm-consumer.v1/conv"
	"strings"
)

type RowEvent struct {
	ID     int64
	Schema string
	Table  string
	Alias  string
	// the row before update
	PreviousRow map[string]any
	// row for inserted, updated, before delete
	Row      map[string]any
	DiffCols []string
	Action   string

	table *Table
}

// Different column type
const (
	TYPE_NUMBER    = iota + 1 // tinyint, smallint, int, bigint, year
	TYPE_FLOAT                // float, double
	TYPE_ENUM                 // enum
	TYPE_SET                  // set
	TYPE_STRING               // char, varchar, etc.
	TYPE_DATETIME             // datetime
	TYPE_TIMESTAMP            // timestamp
	TYPE_DATE                 // date
	TYPE_TIME                 // time
	TYPE_BIT                  // bit
	TYPE_JSON                 // json
	TYPE_DECIMAL              // decimal
	TYPE_MEDIUM_INT
	TYPE_BINARY // binary, varbinary
	TYPE_POINT  // coordinates
)

type TableColumn struct {
	Name       string
	Type       int
	Collation  string
	RawType    string
	IsAuto     bool
	IsUnsigned bool
	IsVirtual  bool
	IsStored   bool
	EnumValues []string
	SetValues  []string
	FixedSize  uint
	MaxSize    uint
}

type Table struct {
	Schema          string
	Name            string
	Columns         []TableColumn
	Indices         []*TableIndex
	PKColumns       []int
	UnsignedColumns []int
}

type TableIndex struct {
	Name        string
	Columns     []string
	Cardinality []uint64
	NoneUnique  uint64
}

func IsColEmpty(colType int, val any) bool {
	if val == nil {
		return true
	}
	switch colType {
	case TYPE_MEDIUM_INT, TYPE_FLOAT, TYPE_NUMBER, TYPE_DECIMAL:
		return val == 0
	case TYPE_DATETIME, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP,
		TYPE_STRING, TYPE_ENUM, TYPE_SET, TYPE_BINARY, TYPE_BIT, TYPE_JSON:
		_v, ok := val.(string)
		if ok {
			return _v == ""
		}
		_b, ok := val.([]byte)
		if ok {
			return len(_b) <= 0
		}
	}

	return false
}

func IsColValueEqual(colType int, isUnsigned bool, v1, v2 any) bool {
	if v1 == nil && v2 == nil {
		return true
	} else if (v1 != nil && v2 == nil) || (v1 == nil && v2 != nil) {
		return false
	}

	same := true
	switch colType {
	case TYPE_MEDIUM_INT, TYPE_NUMBER:
		if isUnsigned {
			same = conv.AnyToUint64(v1) == conv.AnyToUint64(v2)
		} else {
			same = conv.AnyToInt64(v1) == conv.AnyToInt64(v2)
		}
	case TYPE_FLOAT, TYPE_DECIMAL:
		same = conv.AnyToFloat64(v1) == conv.AnyToFloat64(v2)
	case TYPE_DATETIME, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP,
		TYPE_STRING, TYPE_ENUM, TYPE_SET, TYPE_JSON:
		same = conv.AnyToString(v1) == conv.AnyToString(v1)
	case TYPE_BINARY, TYPE_BIT:
		same = bytes.Compare(conv.AnyToBytes(v1), conv.AnyToBytes(v2)) != 0
	case TYPE_POINT:
		// Todo
		same = true
	}

	return same
}

func (t *Table) GetColumn(colName string) *TableColumn {
	for _, col := range t.Columns {
		if strings.EqualFold(col.Name, colName) {
			return &col
		}
	}

	return nil
}

func (e *RowEvent) IsColModified(colName string) bool {
	if _, ok := e.Row[colName]; !ok { // invalid colum name
		return false
	}

	if e.Action == "insert" || e.Action == "update" {
		return true
	}

	for _, v := range e.DiffCols {
		if strings.EqualFold(colName, v) {
			return true
		}
	}
	return false
}

func (e *RowEvent) GetTable() *Table {
	if e.table != nil {
		return e.table
	}

	if GetTableFn == nil {
		return nil
	}

	e.table = GetTableFn(e.Alias)
	return e.table
}

func (e *RowEvent) HasColumn(colName string) bool {
	return e.GetColumn(colName) != nil
}

func (e *RowEvent) GetColumn(colName string) *TableColumn {
	table := e.GetTable()
	if table == nil {
		return nil
	}
	return table.GetColumn(colName)
}

func (e *RowEvent) IsNil(colName string) bool {
	return e.isNil(e.Value2, colName)
}

func (e *RowEvent) IsPreviousNil(colName string) bool {
	return e.isNil(e.PreviousValue2, colName)
}

func (e *RowEvent) isNil(fn func(colName string) (any, bool), colName string) bool {
	val, ok := fn(colName)
	if !ok {
		return false
	}
	return val == nil
}

func (e *RowEvent) IsEmpty(colName string) bool {
	return e.isEmpty(e.Value2, colName)
}

func (e *RowEvent) IsPreviousEmpty(colName string) bool {
	return e.isEmpty(e.PreviousValue2, colName)
}

func (e *RowEvent) isEmpty(fn func(colName string) (any, bool), colName string) bool {
	col := e.GetColumn(colName)
	if col == nil {
		return false
	}

	val, _ := fn(colName)
	return IsColEmpty(col.Type, val)
}

func (e *RowEvent) SetValue(colName string, val any) {
	e.Row[colName] = val
}

func (e *RowEvent) SetPreviousValue(colName string, val any) {
	e.PreviousRow[colName] = val
}

func (e *RowEvent) SetValueByList(colName string, ls []any, val any) {
	i := int(conv.AnyToInt64(val))
	if i < 0 && i >= len(ls) {
		return
	}
	e.SetValue(colName, ls[i])
}

func (e *RowEvent) SetValueByMap(colName string, m map[string]any, val any) {
	var k = conv.AnyToString(val)
	e.SetValue(colName, m[k])
}

func (e *RowEvent) SetValueAsBool(colName string, val any) {
	b := conv.AnyToBool(val)
	e.SetValue(colName, b)
}

func (e *RowEvent) SetValueAsDecodeJson(colName string, val any) {
	s := conv.AnyToString(val)
	var actual any
	json.Unmarshal([]byte(s), &actual)
	e.SetValue(colName, actual)
}

func (e *RowEvent) Value(colName string) any {
	val, _ := e.Value2(colName)
	return val
}

func (e *RowEvent) PreviousValue(colName string) any {
	val, _ := e.PreviousValue2(colName)
	return val
}

func (e *RowEvent) Value2(colName string) (any, bool) {
	val, ok := e.Row[colName]
	return val, ok
}

func (e *RowEvent) PreviousValue2(colName string) (any, bool) {
	val, ok := e.PreviousRow[colName]
	return val, ok
}

func (e *RowEvent) Equal(colName string, val any) bool {
	return e.equal(e.Value2, colName, val)
}

func (e *RowEvent) PreviousEqual(colName string, val any) bool {
	return e.equal(e.PreviousValue2, colName, val)
}

func (e *RowEvent) equal(fn func(colName string) (any, bool), colName string, v2 any) bool {
	col := e.GetColumn(colName)
	if col == nil { // 键名不存在
		return false
	}

	v1, _ := fn(colName)
	return IsColValueEqual(col.Type, col.IsUnsigned, v1, v2)
}
