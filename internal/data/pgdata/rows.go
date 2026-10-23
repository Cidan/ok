package pgdata

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/data"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// A move's rows (docs/hosted.md, "Cells, as built"): a role's tables, read and written whole, a
// page at a time, as rows of ok.v1.Row named by the columns — PostgreSQL's names, which a move
// carries between databases. What each table is — its columns, key, and references — is read from
// the catalog of the schema the connection is in, so no table is described twice.

// RowsQuerier runs statements and reads rows: a pool, a connection, or a transaction.
type RowsQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Table is a table as the catalog describes it.
type Table struct {
	Name    string
	Columns []Column
	Key     []string // its primary key's columns, in order
	// Refs are the other tables its foreign keys reference; SelfRefs its columns that reference
	// its own rows.
	Refs     []string
	SelfRefs []string
}

// Column is a table's column.
type Column struct {
	Name, Type, Elem string // Type is information_schema's data_type; Elem an array's element (udt_name)
	// Generated: computed, never written. Identity: GENERATED ALWAYS, written only overriding it.
	Generated, Identity bool
	// Sequence is the sequence that numbers it, if one does: its own (an identity, a serial), or one
	// its default draws from.
	Sequence string
}

func (t *Table) column(name string) (Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// Tables reads the catalog's description of tables of the connection's schema.
func Tables(ctx context.Context, q RowsQuerier, names []string) (map[string]*Table, error) {
	out := map[string]*Table{}
	for _, n := range names {
		out[n] = &Table{Name: n}
	}
	rows, err := q.Query(ctx, `SELECT table_name, column_name, data_type, udt_name, is_identity = 'YES' AND identity_generation = 'ALWAYS',
			is_generated = 'ALWAYS', coalesce(pg_get_serial_sequence(quote_ident(table_name), column_name),
				substring(column_default FROM 'nextval\(''([^'']+)''::regclass\)'), '')
		FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ANY($1) ORDER BY table_name, ordinal_position`, names)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t string
		var c Column
		if err := rows.Scan(&t, &c.Name, &c.Type, &c.Elem, &c.Identity, &c.Generated, &c.Sequence); err != nil {
			rows.Close()
			return nil, err
		}
		out[t].Columns = append(out[t].Columns, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = q.Query(ctx, `SELECT c.conrelid::regclass::text, c.confrelid::regclass::text, c.contype::text,
			ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(n, i) JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n ORDER BY k.i)
		FROM pg_constraint c WHERE c.contype IN ('p', 'f') AND c.connamespace = current_schema()::regnamespace`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var t, ref, kind string
		var cols []string
		if err := rows.Scan(&t, &ref, &kind, &cols); err != nil {
			return nil, err
		}
		tb, ok := out[t]
		if !ok {
			continue
		}
		switch {
		case kind == "p":
			tb.Key = cols
		case ref == t:
			tb.SelfRefs = append(tb.SelfRefs, cols...)
		default:
			tb.Refs = append(tb.Refs, ref)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, t := range out {
		if len(t.Columns) == 0 {
			return nil, fmt.Errorf("pgdata: no table %s", t.Name)
		}
		if len(t.Key) == 0 {
			return nil, fmt.Errorf("pgdata: table %s has no primary key", t.Name)
		}
	}
	return out, nil
}

// ParentsFirst orders tables so each comes after those of them it references.
func ParentsFirst(tables map[string]*Table) ([]string, error) {
	var names []string
	for n := range tables {
		names = append(names, n)
	}
	sort.Strings(names)
	var out []string
	state := map[string]int{} // 1 visiting, 2 done
	var visit func(n string) error
	visit = func(n string) error {
		switch state[n] {
		case 1:
			return fmt.Errorf("pgdata: tables reference each other in a cycle through %s", n)
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

// keyAfter is SQL selecting rows past after by key, and its arguments, named mk0….
func keyAfter(key []string, after *okpb.Row, args pgx.NamedArgs) (string, error) {
	if after == nil || len(after.GetColumns()) == 0 {
		return "TRUE", nil
	}
	vals := map[string]*okpb.Value{}
	for _, c := range after.GetColumns() {
		vals[c.GetName()] = c.GetValue()
	}
	var ors []string
	for i := range key {
		var ands []string
		for j := 0; j <= i; j++ {
			v, ok := vals[key[j]]
			if !ok {
				return "", fmt.Errorf("pgdata: the row to start after lacks key column %s", key[j])
			}
			name := fmt.Sprintf("mk%d", j)
			args[name] = goValue(v)
			op := "="
			if j == i {
				op = ">"
			}
			ands = append(ands, fmt.Sprintf("%s %s @%s", quote(key[j]), op, name))
		}
		ors = append(ors, "("+strings.Join(ands, " AND ")+")")
	}
	return "(" + strings.Join(ors, " OR ") + ")", nil
}

// ExportRows reads up to limit of a table's rows where where holds (with args), after the row with
// key after, in key order.
func ExportRows(ctx context.Context, q RowsQuerier, t *Table, where string, args pgx.NamedArgs, after *okpb.Row, limit int) ([]*okpb.Row, error) {
	a := pgx.NamedArgs{}
	for k, v := range args {
		a[k] = v
	}
	cond, err := keyAfter(t.Key, after, a)
	if err != nil {
		return nil, err
	}
	var cols []string
	for _, c := range t.Columns {
		cols = append(cols, quote(c.Name))
	}
	var order []string
	for _, k := range t.Key {
		order = append(order, quote(k))
	}
	rows, err := q.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE (%s) AND %s ORDER BY %s LIMIT %d`,
		strings.Join(cols, ", "), quote(t.Name), where, cond, strings.Join(order, ", "), limit), a)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*okpb.Row
	fields := rows.FieldDescriptions()
	for rows.Next() {
		vs, err := rows.Values()
		if err != nil {
			return nil, err
		}
		raw := rows.RawValues()
		row := &okpb.Row{}
		for i, c := range t.Columns {
			if c.Type == "json" || c.Type == "jsonb" {
				// As stored: decoded, a JSON null would read as no value.
				v, err := jsonValue(c, fields[i].Format, raw[i])
				if err != nil {
					return nil, fmt.Errorf("%s.%s: %w", t.Name, c.Name, err)
				}
				row.Columns = append(row.Columns, &okpb.Column{Name: c.Name, Value: v})
				continue
			}
			v, err := neutral(c, vs[i])
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", t.Name, c.Name, err)
			}
			row.Columns = append(row.Columns, &okpb.Column{Name: c.Name, Value: v})
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// ImportRows writes rows to a table, over any of the same key: its columns the rows carry, but
// those computed; each column that references the table's own rows after every row is written, as
// a row's parent may come after it by key.
func ImportRows(ctx context.Context, q RowsQuerier, t *Table, rows []*okpb.Row) error {
	if len(rows) == 0 {
		return nil
	}
	self := map[string]bool{}
	for _, c := range t.SelfRefs {
		self[c] = true
	}
	var cols []string
	for _, c := range rows[0].GetColumns() {
		if col, ok := t.column(c.GetName()); ok && !col.Generated {
			cols = append(cols, c.GetName())
		}
	}
	isKey := map[string]bool{}
	for _, k := range t.Key {
		isKey[k] = true
	}
	overriding := ""
	var set []string
	for _, c := range cols {
		col, _ := t.column(c)
		if col.Identity {
			overriding = " OVERRIDING SYSTEM VALUE"
		}
		// An identity column's value is never changed, by a move either.
		if !isKey[c] && !col.Identity {
			set = append(set, fmt.Sprintf("%s = EXCLUDED.%s", quote(c), quote(c)))
		}
	}
	conflict := "DO NOTHING"
	if len(set) > 0 {
		conflict = "DO UPDATE SET " + strings.Join(set, ", ")
	}
	var keyCols []string
	for _, k := range t.Key {
		keyCols = append(keyCols, quote(k))
	}
	var quoted []string
	for _, c := range cols {
		quoted = append(quoted, quote(c))
	}
	perStatement := max(1, 30_000/len(cols))
	for start := 0; start < len(rows); start += perStatement {
		batch := rows[start:min(start+perStatement, len(rows))]
		var tuples []string
		var args []any
		for _, r := range batch {
			vals := valuesByName(r)
			var ph []string
			for _, c := range cols {
				col, _ := t.column(c)
				v := vals[c]
				if self[c] {
					v = nil // written in the second pass
				}
				gv, err := pgValue(col, v)
				if err != nil {
					return fmt.Errorf("%s.%s: %w", t.Name, c, err)
				}
				args = append(args, gv)
				ph = append(ph, fmt.Sprintf("$%d", len(args)))
			}
			tuples = append(tuples, "("+strings.Join(ph, ", ")+")")
		}
		if _, err := q.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (%s)%s VALUES %s ON CONFLICT (%s) %s`, quote(t.Name), strings.Join(quoted, ", "),
			overriding, strings.Join(tuples, ", "), strings.Join(keyCols, ", "), conflict), args...); err != nil {
			return fmt.Errorf("importing %s: %w", t.Name, err)
		}
	}
	for c := range self {
		col, _ := t.column(c)
		for _, r := range rows {
			vals := valuesByName(r)
			v := vals[c]
			if v == nil || v.GetNull() {
				continue
			}
			gv, err := pgValue(col, v)
			if err != nil {
				return err
			}
			args := []any{gv}
			var where []string
			for _, k := range t.Key {
				kc, _ := t.column(k)
				kv, err := pgValue(kc, vals[k])
				if err != nil {
					return err
				}
				args = append(args, kv)
				where = append(where, fmt.Sprintf("%s = $%d", quote(k), len(args)))
			}
			if _, err := q.Exec(ctx, fmt.Sprintf(`UPDATE %s SET %s = $1 WHERE %s`, quote(t.Name), quote(c), strings.Join(where, " AND ")), args...); err != nil {
				return fmt.Errorf("importing %s.%s: %w", t.Name, c, err)
			}
		}
	}
	return nil
}

// AdvanceSequences moves each sequence that numbers a table's columns past the largest value the
// table holds: rows written with their numbers, as a move writes them, leave it behind.
func AdvanceSequences(ctx context.Context, q RowsQuerier, t *Table) error {
	for _, c := range t.Columns {
		if c.Sequence == "" {
			continue
		}
		if _, err := q.Exec(ctx, fmt.Sprintf(`SELECT setval($1::regclass, m) FROM (SELECT max(%s) AS m FROM %s) x
			WHERE m IS NOT NULL AND m > coalesce(pg_sequence_last_value($1::regclass), 0)`, quote(c.Name), quote(t.Name)), c.Sequence); err != nil {
			return fmt.Errorf("advancing %s: %w", c.Sequence, err)
		}
	}
	return nil
}

// DeleteRows deletes up to limit of a table's rows where where holds, and returns how many.
func DeleteRows(ctx context.Context, q RowsQuerier, table, where string, args pgx.NamedArgs, limit int) (int64, error) {
	tag, err := q.Exec(ctx, fmt.Sprintf(`DELETE FROM %[1]s WHERE ctid = ANY(ARRAY(SELECT ctid FROM %[1]s WHERE %[2]s LIMIT %[3]d))`,
		quote(table), where, limit), args)
	return tag.RowsAffected(), err
}

func quote(ident string) string { return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"` }

func valuesByName(r *okpb.Row) map[string]*okpb.Value {
	m := make(map[string]*okpb.Value, len(r.GetColumns()))
	for _, c := range r.GetColumns() {
		m[c.GetName()] = c.GetValue()
	}
	return m
}

// jsonValue is a JSON column's value as the server sent it — jsonb's binary form is a version byte
// and the text — canonical.
func jsonValue(c Column, format int16, raw []byte) (*okpb.Value, error) {
	if raw == nil {
		return &okpb.Value{Kind: &okpb.Value_Null{Null: true}}, nil
	}
	if c.Type == "jsonb" && format == pgx.BinaryFormatCode && len(raw) > 0 {
		raw = raw[1:]
	}
	j, err := data.CanonicalJSON(string(raw))
	if err != nil {
		return nil, err
	}
	return &okpb.Value{Kind: &okpb.Value_Json{Json: j}}, nil
}

// neutral is a value pgx read of a column as a move carries it.
func neutral(c Column, v any) (*okpb.Value, error) {
	if v == nil {
		return &okpb.Value{Kind: &okpb.Value_Null{Null: true}}, nil
	}
	switch c.Type {
	case "date":
		switch d := v.(type) {
		case time.Time:
			return &okpb.Value{Kind: &okpb.Value_Date{Date: d.Format(time.DateOnly)}}, nil
		case pgtype.Date:
			return &okpb.Value{Kind: &okpb.Value_Date{Date: d.Time.Format(time.DateOnly)}}, nil
		}
	}
	switch x := v.(type) {
	case int64:
		return &okpb.Value{Kind: &okpb.Value_Int{Int: x}}, nil
	case int32:
		return &okpb.Value{Kind: &okpb.Value_Int{Int: int64(x)}}, nil
	case int16:
		return &okpb.Value{Kind: &okpb.Value_Int{Int: int64(x)}}, nil
	case float64:
		return &okpb.Value{Kind: &okpb.Value_Float{Float: x}}, nil
	case float32:
		return &okpb.Value{Kind: &okpb.Value_Float{Float: float64(x)}}, nil
	case string:
		return &okpb.Value{Kind: &okpb.Value_Text{Text: x}}, nil
	case []byte:
		return &okpb.Value{Kind: &okpb.Value_Data{Data: x}}, nil
	case bool:
		return &okpb.Value{Kind: &okpb.Value_Flag{Flag: x}}, nil
	case time.Time:
		return &okpb.Value{Kind: &okpb.Value_Time{Time: timestamppb.New(x)}}, nil
	case []any:
		return neutralArray(c, x)
	}
	return nil, fmt.Errorf("a %s value of type %T", c.Type, v)
}

func neutralArray(c Column, xs []any) (*okpb.Value, error) {
	switch c.Elem {
	case "_int8", "_int4", "_int2":
		out := &okpb.Ints{}
		for _, x := range xs {
			switch n := x.(type) {
			case int64:
				out.Values = append(out.Values, n)
			case int32:
				out.Values = append(out.Values, int64(n))
			case int16:
				out.Values = append(out.Values, int64(n))
			default:
				return nil, fmt.Errorf("an integer array holding %T", x)
			}
		}
		return &okpb.Value{Kind: &okpb.Value_Ints{Ints: out}}, nil
	case "_text", "_varchar":
		out := &okpb.Texts{}
		for _, x := range xs {
			s, ok := x.(string)
			if !ok {
				return nil, fmt.Errorf("a text array holding %T", x)
			}
			out.Values = append(out.Values, s)
		}
		return &okpb.Value{Kind: &okpb.Value_Texts{Texts: out}}, nil
	case "_bytea":
		out := &okpb.Datas{}
		for _, x := range xs {
			b, ok := x.([]byte)
			if !ok {
				return nil, fmt.Errorf("a bytea array holding %T", x)
			}
			out.Values = append(out.Values, b)
		}
		return &okpb.Value{Kind: &okpb.Value_Datas{Datas: out}}, nil
	}
	return nil, fmt.Errorf("an array of %s", c.Elem)
}

// goValue is a carried value as Go holds it, for a key's comparison.
func goValue(v *okpb.Value) any {
	switch k := v.GetKind().(type) {
	case *okpb.Value_Int:
		return k.Int
	case *okpb.Value_Float:
		return k.Float
	case *okpb.Value_Text:
		return k.Text
	case *okpb.Value_Data:
		return append([]byte{}, k.Data...)
	case *okpb.Value_Flag:
		return k.Flag
	case *okpb.Value_Time:
		return k.Time.AsTime()
	case *okpb.Value_Date:
		d, _ := time.Parse(time.DateOnly, k.Date)
		return d
	case *okpb.Value_Json:
		return json.RawMessage(k.Json)
	case *okpb.Value_Ints: // nil would write NULL, not an empty array
		return append([]int64{}, k.Ints.GetValues()...)
	case *okpb.Value_Texts:
		return append([]string{}, k.Texts.GetValues()...)
	case *okpb.Value_Datas:
		return append([][]byte{}, k.Datas.GetValues()...)
	}
	return nil
}

// pgValue is a carried value as a column takes it.
func pgValue(c Column, v *okpb.Value) (any, error) {
	if v == nil || v.GetNull() {
		return nil, nil
	}
	switch c.Type {
	case "json", "jsonb":
		switch k := v.GetKind().(type) {
		case *okpb.Value_Json:
			return json.RawMessage(k.Json), nil
		case *okpb.Value_Text:
			return json.RawMessage(k.Text), nil
		}
	case "text", "character varying":
		if k, ok := v.GetKind().(*okpb.Value_Json); ok {
			return k.Json, nil
		}
	case "date":
		switch k := v.GetKind().(type) {
		case *okpb.Value_Date:
			return time.Parse(time.DateOnly, k.Date)
		case *okpb.Value_Time:
			return k.Time.AsTime(), nil
		}
	}
	return goValue(v), nil
}
