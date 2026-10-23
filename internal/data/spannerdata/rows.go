package spannerdata

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannertxn"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// A move's rows (docs/hosted.md, "Cells, as built"), as pgdata's: a role's tables read and written
// whole, a page at a time, as rows of ok.v1.Row named by PostgreSQL's columns — which a role maps
// to its Spanner columns where they differ (Renames) — each table described by the catalog.

// Renames are, by table, the columns a Spanner schema names otherwise than PostgreSQL's: Spanner's
// name by PostgreSQL's.
type Renames map[string]map[string]string

// Table is a table as the catalog describes it, its columns by their Spanner names.
type Table struct {
	Name    string
	Columns []Column
	Key     []string
	// Refs are the other tables it is interleaved in or its foreign keys reference; SelfRefs its
	// columns that reference its own rows.
	Refs     []string
	SelfRefs []string
	// logical are its columns' PostgreSQL names, by Spanner's; physical the reverse.
	logical, physical map[string]string
}

// Column is a table's column.
type Column struct {
	Name, Type string // Type is Spanner's: INT64, STRING(MAX), ARRAY<INT64>, …
	Generated  bool
}

func (t *Table) column(name string) (Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// Logical is a column's PostgreSQL name.
func (t *Table) Logical(c string) string {
	if l, ok := t.logical[c]; ok {
		return l
	}
	return c
}

// Physical is a column's Spanner name, by its PostgreSQL name.
func (t *Table) Physical(c string) string {
	if p, ok := t.physical[c]; ok {
		return p
	}
	return c
}

// Tables reads the catalog's description of tables.
func Tables(ctx context.Context, r Reader, names []string, renames Renames) (map[string]*Table, error) {
	out := map[string]*Table{}
	for _, n := range names {
		t := &Table{Name: n, logical: map[string]string{}, physical: map[string]string{}}
		for pg, sp := range renames[n] {
			t.logical[sp], t.physical[pg] = pg, sp
		}
		out[n] = t
	}
	p := map[string]any{"names": names}
	if err := Each(ctx, r, `SELECT TABLE_NAME, COLUMN_NAME, SPANNER_TYPE, IS_GENERATED = 'ALWAYS' FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = '' AND TABLE_NAME IN UNNEST(@names) ORDER BY TABLE_NAME, ORDINAL_POSITION`, p, func(row *spanner.Row) error {
		var t string
		var c Column
		if err := row.Columns(&t, &c.Name, &c.Type, &c.Generated); err != nil {
			return err
		}
		out[t].Columns = append(out[t].Columns, c)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := Each(ctx, r, `SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS
		WHERE TABLE_SCHEMA = '' AND INDEX_NAME = 'PRIMARY_KEY' AND TABLE_NAME IN UNNEST(@names) ORDER BY TABLE_NAME, ORDINAL_POSITION`, p,
		func(row *spanner.Row) error {
			var t, c string
			if err := row.Columns(&t, &c); err != nil {
				return err
			}
			out[t].Key = append(out[t].Key, c)
			return nil
		}); err != nil {
		return nil, err
	}
	if err := Each(ctx, r, `SELECT TABLE_NAME, IFNULL(PARENT_TABLE_NAME, '') FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = '' AND TABLE_NAME IN UNNEST(@names)`, p, func(row *spanner.Row) error {
		var t, parent string
		if err := row.Columns(&t, &parent); err != nil {
			return err
		}
		if parent != "" {
			out[t].Refs = append(out[t].Refs, parent)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// Foreign keys: each constraint's table and columns, and the table its referenced key is of.
	if err := Each(ctx, r, `SELECT tc.TABLE_NAME, ARRAY(SELECT k.COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
			WHERE k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME ORDER BY k.ORDINAL_POSITION), u.TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ON rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
		JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS u ON u.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA AND u.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
		WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' AND tc.TABLE_SCHEMA = '' AND tc.TABLE_NAME IN UNNEST(@names)`, p, func(row *spanner.Row) error {
		var t, ref string
		var cols []string
		if err := row.Columns(&t, &cols, &ref); err != nil {
			return err
		}
		if ref == t {
			out[t].SelfRefs = append(out[t].SelfRefs, cols...)
		} else {
			out[t].Refs = append(out[t].Refs, ref)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	for _, t := range out {
		if len(t.Columns) == 0 {
			return nil, fmt.Errorf("spannerdata: no table %s", t.Name)
		}
	}
	return out, nil
}

// ParentsFirst orders tables so each comes after those of them it references or is interleaved in.
func ParentsFirst(tables map[string]*Table) ([]string, error) {
	var names []string
	for n := range tables {
		names = append(names, n)
	}
	sort.Strings(names)
	var out []string
	state := map[string]int{}
	var visit func(n string) error
	visit = func(n string) error {
		switch state[n] {
		case 1:
			return fmt.Errorf("spannerdata: tables reference each other in a cycle through %s", n)
		case 2:
			return nil
		}
		state[n] = 1
		refs := append([]string(nil), tables[n].Refs...)
		sort.Strings(refs)
		for _, r := range refs {
			if _, ok := tables[r]; ok {
				if err := visit(r); err != nil {
					return err
				}
			}
		}
		state[n] = 2
		out = append(out, n)
		return nil
	}
	for _, n := range names {
		if err := visit(n); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ExportRows reads up to limit of a table's rows where where holds, after the row with key after
// (by PostgreSQL's names), in key order; each named by PostgreSQL's columns, without the columns
// Spanner alone has (skip, by Spanner's names).
func ExportRows(ctx context.Context, r Reader, t *Table, where string, params map[string]any, after *okpb.Row, limit int,
	skip map[string]bool) ([]*okpb.Row, error) {
	p := map[string]any{}
	for k, v := range params {
		p[k] = v
	}
	cond := "TRUE"
	if after != nil && len(after.GetColumns()) > 0 {
		vals := map[string]*okpb.Value{}
		for _, c := range after.GetColumns() {
			vals[t.Physical(c.GetName())] = c.GetValue()
		}
		var ors []string
		for i := range t.Key {
			var ands []string
			for j := 0; j <= i; j++ {
				v, ok := vals[t.Key[j]]
				if !ok {
					return nil, fmt.Errorf("spannerdata: the row to start after lacks key column %s", t.Key[j])
				}
				col, _ := t.column(t.Key[j])
				name := fmt.Sprintf("mk%d", j)
				gv, err := spannerValue(col, v)
				if err != nil {
					return nil, err
				}
				p[name] = gv
				op := "="
				if j == i {
					op = ">"
				}
				ands = append(ands, fmt.Sprintf("%s %s @%s", t.Key[j], op, name))
			}
			ors = append(ors, "("+strings.Join(ands, " AND ")+")")
		}
		cond = "(" + strings.Join(ors, " OR ") + ")"
	}
	var cols []string
	var kept []Column
	for _, c := range t.Columns {
		if skip[c.Name] {
			continue
		}
		cols = append(cols, c.Name)
		kept = append(kept, c)
	}
	var out []*okpb.Row
	err := Each(ctx, r, fmt.Sprintf(`SELECT %s FROM %s WHERE (%s) AND %s ORDER BY %s LIMIT %d`, strings.Join(cols, ", "), t.Name, where, cond,
		strings.Join(t.Key, ", "), limit), p, func(row *spanner.Row) error {
		r := &okpb.Row{}
		for i, c := range kept {
			var g spanner.GenericColumnValue
			if err := row.Column(i, &g); err != nil {
				return err
			}
			v, err := neutral(g)
			if err != nil {
				return fmt.Errorf("%s.%s: %w", t.Name, c.Name, err)
			}
			r.Columns = append(r.Columns, &okpb.Column{Name: t.Logical(c.Name), Value: v})
		}
		out = append(out, r)
		return nil
	})
	return out, err
}

// mutationCells bounds the column values one commit of an import writes: Spanner holds a commit to
// 80,000 mutations, each index entry one more.
const mutationCells = 16_000

// ImportRows writes rows to a table over any of the same key — the columns the table has but
// those computed, each named by PostgreSQL's columns — in commits of their own; each column that
// references the table's own rows after every row is written, as a row's parent may come after it
// by key. fill, when set, sets a row's columns that Spanner alone has, from the rest.
func ImportRows(ctx context.Context, c *spanner.Client, t *Table, rows []*okpb.Row, fill func(map[string]*okpb.Value)) error {
	if len(rows) == 0 {
		return nil
	}
	self := map[string]bool{}
	for _, s := range t.SelfRefs {
		self[s] = true
	}
	var ms []*spanner.Mutation
	cells := 0
	flush := func() error {
		if len(ms) == 0 {
			return nil
		}
		_, err := c.Apply(ctx, ms)
		ms, cells = nil, 0
		if err != nil {
			return fmt.Errorf("importing %s: %w", t.Name, err)
		}
		return nil
	}
	var later []map[string]*okpb.Value
	for _, r := range rows {
		vals := map[string]*okpb.Value{}
		for _, col := range r.GetColumns() {
			vals[t.Physical(col.GetName())] = col.GetValue()
		}
		if fill != nil {
			fill(vals)
		}
		var cols []string
		var gvs []any
		hasSelf := false
		for _, col := range t.Columns {
			v, ok := vals[col.Name]
			if !ok || col.Generated {
				continue
			}
			if self[col.Name] && v != nil && !v.GetNull() {
				hasSelf = true
				v = nil
			}
			gv, err := spannerValue(col, v)
			if err != nil {
				return fmt.Errorf("%s.%s: %w", t.Name, col.Name, err)
			}
			cols = append(cols, col.Name)
			gvs = append(gvs, gv)
		}
		if hasSelf {
			later = append(later, vals)
		}
		ms = append(ms, spanner.InsertOrUpdate(t.Name, cols, gvs))
		if cells += len(cols); cells >= mutationCells {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	for _, vals := range later {
		var cols []string
		var gvs []any
		for _, k := range t.Key {
			col, _ := t.column(k)
			gv, err := spannerValue(col, vals[k])
			if err != nil {
				return err
			}
			cols, gvs = append(cols, k), append(gvs, gv)
		}
		for s := range self {
			col, _ := t.column(s)
			gv, err := spannerValue(col, vals[s])
			if err != nil {
				return err
			}
			cols, gvs = append(cols, s), append(gvs, gv)
		}
		ms = append(ms, spanner.Update(t.Name, cols, gvs))
		if cells += len(cols); cells >= mutationCells {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// DeleteRows deletes up to limit of a table's rows where where holds, in a transaction of its own,
// a page of rowsPerDelete at a time, and returns how many.
func DeleteRows(ctx context.Context, c *spanner.Client, t *Table, where string, params map[string]any, limit int) (int64, error) {
	var typed []string
	for _, k := range t.Key {
		col, _ := t.column(k)
		typed = append(typed, k+" "+baseType(col.Type))
	}
	key := strings.Join(t.Key, ", ")
	sql := fmt.Sprintf(`DELETE FROM %[1]s WHERE STRUCT<%[2]s>(%[3]s) IN (SELECT AS STRUCT %[3]s FROM %[1]s AS d WHERE %[4]s LIMIT %[5]d)`,
		t.Name, strings.Join(typed, ", "), key, where, limit)
	var n int64
	_, err := spannertxn.ReadWrite(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = tx.Update(ctx, spanner.Statement{SQL: sql, Params: params})
		return err
	})
	return n, err
}

// baseType is a column type without its length: a STRUCT's field takes STRING, not STRING(MAX).
func baseType(t string) string {
	if i := strings.IndexByte(t, '('); i >= 0 && !strings.HasPrefix(t, "ARRAY") {
		return t[:i]
	}
	return t
}

// neutral is a column's value as a move carries it.
func neutral(g spanner.GenericColumnValue) (*okpb.Value, error) {
	null := &okpb.Value{Kind: &okpb.Value_Null{Null: true}}
	if _, isNull := g.Value.GetKind().(*structpb.Value_NullValue); isNull {
		return null, nil
	}
	switch g.Type.GetCode() {
	case sppb.TypeCode_INT64:
		var v spanner.NullInt64
		if err := g.Decode(&v); err != nil || !v.Valid {
			return null, err
		}
		return &okpb.Value{Kind: &okpb.Value_Int{Int: v.Int64}}, nil
	case sppb.TypeCode_FLOAT64:
		var v spanner.NullFloat64
		if err := g.Decode(&v); err != nil || !v.Valid {
			return null, err
		}
		return &okpb.Value{Kind: &okpb.Value_Float{Float: v.Float64}}, nil
	case sppb.TypeCode_STRING:
		var v spanner.NullString
		if err := g.Decode(&v); err != nil || !v.Valid {
			return null, err
		}
		return &okpb.Value{Kind: &okpb.Value_Text{Text: v.StringVal}}, nil
	case sppb.TypeCode_BYTES:
		var v []byte
		if err := g.Decode(&v); err != nil {
			return nil, err
		}
		return &okpb.Value{Kind: &okpb.Value_Data{Data: v}}, nil
	case sppb.TypeCode_BOOL:
		var v spanner.NullBool
		if err := g.Decode(&v); err != nil || !v.Valid {
			return null, err
		}
		return &okpb.Value{Kind: &okpb.Value_Flag{Flag: v.Bool}}, nil
	case sppb.TypeCode_TIMESTAMP:
		var v spanner.NullTime
		if err := g.Decode(&v); err != nil || !v.Valid {
			return null, err
		}
		return &okpb.Value{Kind: &okpb.Value_Time{Time: timestamppb.New(v.Time)}}, nil
	case sppb.TypeCode_DATE:
		var v spanner.NullDate
		if err := g.Decode(&v); err != nil || !v.Valid {
			return null, err
		}
		return &okpb.Value{Kind: &okpb.Value_Date{Date: v.Date.String()}}, nil
	case sppb.TypeCode_JSON:
		// Its text, not a decoding of it, which would round large numbers.
		j, err := data.CanonicalJSON(g.Value.GetStringValue())
		if err != nil {
			return nil, err
		}
		return &okpb.Value{Kind: &okpb.Value_Json{Json: j}}, nil
	case sppb.TypeCode_ARRAY:
		switch g.Type.GetArrayElementType().GetCode() {
		case sppb.TypeCode_INT64:
			var v []int64
			if err := g.Decode(&v); err != nil {
				return nil, err
			}
			return &okpb.Value{Kind: &okpb.Value_Ints{Ints: &okpb.Ints{Values: v}}}, nil
		case sppb.TypeCode_STRING:
			var v []string
			if err := g.Decode(&v); err != nil {
				return nil, err
			}
			return &okpb.Value{Kind: &okpb.Value_Texts{Texts: &okpb.Texts{Values: v}}}, nil
		case sppb.TypeCode_BYTES:
			var v [][]byte
			if err := g.Decode(&v); err != nil {
				return nil, err
			}
			return &okpb.Value{Kind: &okpb.Value_Datas{Datas: &okpb.Datas{Values: v}}}, nil
		}
	}
	return nil, fmt.Errorf("a value of type %s", g.Type)
}

// spannerValue is a carried value as a column of a type takes it, a typed NULL for none.
func spannerValue(c Column, v *okpb.Value) (any, error) {
	null := v == nil || v.GetNull()
	t := c.Type
	switch {
	case t == "INT64":
		if null {
			return spanner.NullInt64{}, nil
		}
		if k, ok := v.GetKind().(*okpb.Value_Int); ok {
			return k.Int, nil
		}
	case t == "FLOAT64":
		if null {
			return spanner.NullFloat64{}, nil
		}
		switch k := v.GetKind().(type) {
		case *okpb.Value_Float:
			return k.Float, nil
		case *okpb.Value_Int:
			return float64(k.Int), nil
		}
	case strings.HasPrefix(t, "STRING"):
		if null {
			return spanner.NullString{}, nil
		}
		switch k := v.GetKind().(type) {
		case *okpb.Value_Text:
			return k.Text, nil
		case *okpb.Value_Json:
			return k.Json, nil
		case *okpb.Value_Date:
			return k.Date, nil
		}
	case strings.HasPrefix(t, "BYTES"):
		if null {
			return []byte(nil), nil
		}
		if k, ok := v.GetKind().(*okpb.Value_Data); ok {
			return append([]byte{}, k.Data...), nil // nil would write NULL, not empty bytes
		}
	case t == "BOOL":
		if null {
			return spanner.NullBool{}, nil
		}
		if k, ok := v.GetKind().(*okpb.Value_Flag); ok {
			return k.Flag, nil
		}
	case t == "TIMESTAMP":
		if null {
			return spanner.NullTime{}, nil
		}
		if k, ok := v.GetKind().(*okpb.Value_Time); ok {
			return k.Time.AsTime(), nil
		}
	case t == "DATE":
		if null {
			return spanner.NullDate{}, nil
		}
		switch k := v.GetKind().(type) {
		case *okpb.Value_Date:
			d, err := civil.ParseDate(k.Date)
			return d, err
		case *okpb.Value_Time:
			return civil.DateOf(k.Time.AsTime()), nil
		}
	case t == "JSON":
		if null {
			return spanner.NullJSON{}, nil
		}
		var raw string
		switch k := v.GetKind().(type) {
		case *okpb.Value_Json:
			raw = k.Json
		case *okpb.Value_Text:
			raw = k.Text
		default:
			return nil, fmt.Errorf("a %T for a JSON column", v.GetKind())
		}
		if !json.Valid([]byte(raw)) {
			return nil, fmt.Errorf("a JSON column's value is no JSON: %q", raw)
		}
		return spanner.NullJSON{Value: json.RawMessage(raw), Valid: true}, nil
	case t == "ARRAY<INT64>":
		if null {
			return []int64(nil), nil
		}
		if k, ok := v.GetKind().(*okpb.Value_Ints); ok {
			return append([]int64{}, k.Ints.GetValues()...), nil // nil would write NULL, not an empty array
		}
	case strings.HasPrefix(t, "ARRAY<STRING"):
		if null {
			return []string(nil), nil
		}
		if k, ok := v.GetKind().(*okpb.Value_Texts); ok {
			return append([]string{}, k.Texts.GetValues()...), nil
		}
	case strings.HasPrefix(t, "ARRAY<BYTES"):
		if null {
			return [][]byte(nil), nil
		}
		if k, ok := v.GetKind().(*okpb.Value_Datas); ok {
			return append([][]byte{}, k.Datas.GetValues()...), nil
		}
	}
	return nil, fmt.Errorf("a %T for a %s column", v.GetKind(), t)
}
