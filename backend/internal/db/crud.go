package db

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// Generic row writes for the model structs (columns from `db` tags), so services can work
// like the ORM did: load, change fields, save.

type fieldInfo struct {
	columns []string
	index   [][]int
}

var fieldCache sync.Map

func fields(t reflect.Type) fieldInfo {
	if v, ok := fieldCache.Load(t); ok {
		return v.(fieldInfo)
	}
	var fi fieldInfo
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}
		fi.columns = append(fi.columns, tag)
		fi.index = append(fi.index, f.Index)
	}
	fieldCache.Store(t, fi)
	return fi
}

func quoteCol(c string) string { return `"` + c + `"` }

// Insert writes a new row from a model struct pointer.
func Insert(ctx context.Context, q Querier, table string, row any) error {
	v := reflect.ValueOf(row).Elem()
	fi := fields(v.Type())
	cols := make([]string, len(fi.columns))
	ph := make([]string, len(fi.columns))
	args := make([]any, len(fi.columns))
	n := 0
	for i, c := range fi.columns {
		if c == "id" && v.FieldByIndex(fi.index[i]).Kind() == reflect.Int {
			continue // serial primary key
		}
		cols[n] = quoteCol(c)
		ph[n] = fmt.Sprintf("$%d", n+1)
		args[n] = v.FieldByIndex(fi.index[i]).Interface()
		n++
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols[:n], ", "), strings.Join(ph[:n], ", "))
	_, err := q.Exec(ctx, sql, args[:n]...)
	return err
}

// Update writes every column of a row (by primary key "id" or the given key column).
func Update(ctx context.Context, q Querier, table string, row any, key ...string) error {
	pk := "id"
	if len(key) > 0 {
		pk = key[0]
	}
	v := reflect.ValueOf(row).Elem()
	fi := fields(v.Type())
	var sets []string
	var args []any
	var pkVal any
	for i, c := range fi.columns {
		val := v.FieldByIndex(fi.index[i]).Interface()
		if c == pk {
			pkVal = val
			continue
		}
		args = append(args, val)
		sets = append(sets, fmt.Sprintf("%s = $%d", quoteCol(c), len(args)))
	}
	args = append(args, pkVal)
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d", table, strings.Join(sets, ", "), quoteCol(pk), len(args))
	_, err := q.Exec(ctx, sql, args...)
	return err
}

// Delete removes a row by id.
func Delete(ctx context.Context, q Querier, table string, id any) error {
	_, err := q.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", table), id)
	return err
}
