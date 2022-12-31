package consumer

import (
	"bytes"
	"encoding/json"
	"gopkg.in/go-mixed/dm-consumer.v1/conv"
	"strings"
)

type RowEvent struct {
	ID       uint64
	Schema   string
	Table    string
	Alias    string
	OldRow   map[string]any
	NewRow   map[string]any
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
	case TYPE_DATETIME, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP:
		return val == ""
	case TYPE_STRING, TYPE_ENUM, TYPE_SET, TYPE_BINARY, TYPE_BIT:
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

func IsColValueEqual(colType int, v1, v2 any) bool {
	same := true
	switch colType {
	case TYPE_MEDIUM_INT, TYPE_FLOAT, TYPE_NUMBER, TYPE_DECIMAL:
		same = v1 == v2
	case TYPE_DATETIME, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP:
		same = v1 == v2
	case TYPE_STRING, TYPE_ENUM, TYPE_SET, TYPE_BINARY, TYPE_BIT:
		_, ok1 := v1.(string)
		_, ok2 := v2.(string)
		if v1 == nil && v2 == nil {
			same = true
		} else if (v1 == nil && v2 != nil) || (v1 != nil && v2 == nil) {
			same = false
		} else if ok1 || ok2 {
			same = v1 == v2
		} else {
			same = bytes.Compare(v1.([]byte), v2.([]byte)) != 0
		}
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
	if e.Action == "insert" {
		_, ok := e.NewRow[colName]
		return ok
	} else if e.Action == "delete" {
		_, ok := e.OldRow[colName]
		return ok
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

func (e *RowEvent) IsNewNil(colName string) bool {
	return e.isNil(e.NewValue2, colName)
}

func (e *RowEvent) IsOldNil(colName string) bool {
	return e.isNil(e.OldValue2, colName)
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

func (e *RowEvent) IsNewEmpty(colName string) bool {
	return e.isEmpty(e.NewValue2, colName)
}

func (e *RowEvent) IsOldEmpty(colName string) bool {
	return e.isEmpty(e.OldValue2, colName)
}

func (e *RowEvent) isEmpty(fn func(colName string) (any, bool), colName string) bool {
	col := e.GetColumn(colName)
	if col == nil {
		return false
	}

	val, _ := fn(colName)
	return IsColEmpty(col.Type, val)
}

func (e *RowEvent) Row() map[string]any {
	if e.Action == "delete" {
		return e.OldRow
	}
	return e.NewRow
}

func (e *RowEvent) SetValue(colName string, val any) {
	if e.Action == "delete" {
		e.SetOldValue(colName, val)
	} else {
		e.SetNewValue(colName, val)
	}
}

func (e *RowEvent) SetNewValue(colName string, val any) {
	e.NewRow[colName] = val
}

func (e *RowEvent) SetOldValue(colName string, val any) {
	e.OldRow[colName] = val
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
	if e.Action == "delete" {
		return e.OldValue(colName)
	}
	return e.NewValue(colName)
}

func (e *RowEvent) OldValue(colName string) any {
	val, _ := e.OldValue2(colName)
	return val
}

func (e *RowEvent) NewValue(colName string) any {
	val, _ := e.NewValue2(colName)
	return val
}

func (e *RowEvent) Value2(colName string) (any, bool) {
	if e.Action == "delete" {
		return e.OldValue2(colName)
	}
	return e.NewValue2(colName)
}

func (e *RowEvent) OldValue2(colName string) (any, bool) {
	val, ok := e.OldRow[colName]
	return val, ok
}

func (e *RowEvent) NewValue2(colName string) (any, bool) {
	val, ok := e.NewRow[colName]
	return val, ok
}

func (e *RowEvent) Equal(colName string, val any) bool {
	return e.equal(e.Value2, colName, val)
}

func (e *RowEvent) OldEqual(colName string, val any) bool {
	return e.equal(e.OldValue2, colName, val)
}

func (e *RowEvent) NewEqual(colName string, val any) bool {
	return e.equal(e.NewValue2, colName, val)
}

func (e *RowEvent) equal(fn func(colName string) (any, bool), colName string, v2 any) bool {
	col := e.GetColumn(colName)
	if col != nil { // 键名不存在
		return false
	}

	v1, _ := fn(colName)
	return IsColValueEqual(col.Type, v1, v2)
}
