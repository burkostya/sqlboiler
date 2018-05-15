package drivers

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	// clickhouse driver
	_ "github.com/kshvakov/clickhouse"
	"github.com/pkg/errors"
	"github.com/volatiletech/sqlboiler/bdb"
)

// UInt8AsBool is a global that is set from main.go if a user specifies
// this flag when generating. This flag only applies to Clickhouse so we're using
// a global instead, to avoid breaking the interface. If UInt8AsBool is true
// then UInt8(1) will be mapped in your generated structs to bool opposed to Int8.
var UInt8AsBool bool

// ClickhouseDriver holds the database connection string and a handle
// to the database connection.
type ClickhouseDriver struct {
	connStr string
	dbConn  *sql.DB
}

// ClickhouseDriverConfig is config for clickhouse
type ClickhouseDriverConfig struct {
	Username, Password, Database, Host string
	Port                               int
	ReadTimeout, WriteTimeout          int
	Nagle                              bool
	AltHosts                           []string
	ConnectionOpenStrategy             string
	BlockSize                          int
	Debug                              bool
	Secure, SkipVerify                 bool
}

// NewClickhouseDriver takes the database connection details as parameters and
// returns a pointer to a ClickhouseDriver object. Note that it is required to
// call ClickhouseDriver.Open() and ClickhouseDriver.Close() to open and close
// the database connection once an object has been obtained.
func NewClickhouseDriver(config ClickhouseDriverConfig) *ClickhouseDriver {
	driver := ClickhouseDriver{
		connStr: ClickhouseBuildQueryString(config),
	}

	return &driver
}

// ClickhouseBuildQueryString builds a query string for Clickhouse.
func ClickhouseBuildQueryString(config ClickhouseDriverConfig) string {
	dsn := url.URL{}

	dsn.Scheme = "tcp"

	dsn.Host = fmt.Sprintf("%s:%d", config.Host, config.Port)

	q := url.Values{}
	if config.Username != "" {
		q.Set("username", config.Username)
	}
	if config.Password != "" {
		q.Set("password", config.Password)
	}
	q.Set("database", config.Database)

	if config.ReadTimeout != 0 {
		q.Set("read_timeout", strconv.Itoa(config.ReadTimeout))
	}
	if config.WriteTimeout != 0 {
		q.Set("write_timeout", strconv.Itoa(config.WriteTimeout))
	}

	q.Set("no_delay", strconv.FormatBool(!config.Nagle))

	if len(config.AltHosts) > 0 {
		q.Set("alt_hosts", strings.Join(config.AltHosts, ","))
	}

	if config.ConnectionOpenStrategy != "" {
		q.Set("connection_open_strategy", config.ConnectionOpenStrategy)
	}

	if config.BlockSize > 0 {
		q.Set("block_size", strconv.Itoa(config.BlockSize))
	}

	q.Set("debug", strconv.FormatBool(config.Debug))

	dsn.RawQuery = q.Encode()

	return dsn.String()
}

// Open opens the database connection using the connection string
func (m *ClickhouseDriver) Open() error {
	var err error
	m.dbConn, err = sql.Open("clickhouse", m.connStr)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the database connection
func (m *ClickhouseDriver) Close() {
	m.dbConn.Close()
}

// UseLastInsertID returns false to indicate Clickhouse doesnt support last insert id
func (m *ClickhouseDriver) UseLastInsertID() bool {
	return false
}

// UseTopClause returns false to indicate Clickhouse doesnt support SQL TOP clause
func (m *ClickhouseDriver) UseTopClause() bool {
	return false
}

// TableNames connects to the database and
// retrieves all table names from the system.tables where the
// table schema is public.
func (m *ClickhouseDriver) TableNames(database string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := fmt.Sprintf(`select name from system.tables where database = ? and database <> 'system'`)
	args := []interface{}{database}
	if len(whitelist) > 0 {
		query += fmt.Sprintf(" and name in (%s);", strings.Repeat(",?", len(whitelist))[1:])
		for _, w := range whitelist {
			args = append(args, w)
		}
	} else if len(blacklist) > 0 {
		query += fmt.Sprintf(" and name not in (%s);", strings.Repeat(",?", len(blacklist))[1:])
		for _, b := range blacklist {
			args = append(args, b)
		}
	}

	rows, err := m.dbConn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	return names, nil
}

// Columns takes a table name and attempts to retrieve the table information
// from the database system.columns. It retrieves the column names
// and column types and returns those as a []Column after TranslateColumnType()
// converts the SQL types to Go types, for example: "varchar" to "string"
func (m *ClickhouseDriver) Columns(database, tableName string) ([]bdb.Column, error) {
	var columns []bdb.Column

	rows, err := m.dbConn.Query(`
	select name, type, default_expression
		from system.columns
	where table = ? and database = ?;
	`, tableName, database)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, fullColType string
		var defaultValue string
		if err := rows.Scan(&colName, &fullColType, &defaultValue); err != nil {
			return nil, errors.Wrapf(err, "unable to scan for table %s", tableName)
		}

		colType := fullColType
		idx := strings.Index(fullColType, "(")
		if idx > 0 {
			colType = fullColType[:idx]
		}

		column := bdb.Column{
			Name:       colName,
			FullDBType: fullColType,
			DBType:     colType,
			Default:    defaultValue,
		}

		columns = append(columns, column)
	}

	return columns, nil
}

// PrimaryKeyInfo looks up the primary key for a table.
func (m *ClickhouseDriver) PrimaryKeyInfo(database, table string) (*bdb.PrimaryKey, error) {
	pkey := &bdb.PrimaryKey{}
	var err error

	query := `
	select name, engine_full
	from system.tables
	where name = ? and database = ?;`

	var engineFull string

	row := m.dbConn.QueryRow(query, table, database)
	if err = row.Scan(&pkey.Name, &engineFull); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	engine, err := m.parseEngine(engineFull)
	if err != nil {
		return nil, errors.Wrapf(err, "bad engine=`%s`", engineFull)
	}

	pkey.Columns = engine.PrimaryKey

	return pkey, nil
}

func (m *ClickhouseDriver) parseEngine(str string) (*clickhouseEngine, error) {
	idx := strings.Index(str, "(")
	if idx == -1 {
		return nil, errors.New("open bracket not found")
	}

	engine := clickhouseEngine{}
	engine.Name = str[:idx]

	params := strings.Trim(str[idx:], "() ")

	idx = strings.Index(params, ",")
	if idx == -1 {
		return nil, errors.New("partitioning key not found")
	}

	engine.PartitioningKey = params[:idx]
	params = strings.TrimLeft(params[idx:], ", ")

	idx = strings.LastIndex(params, ",")
	if idx == -1 {
		return nil, errors.New("granularity key not found")
	}

	granularity, err := strconv.Atoi(strings.Trim(params[idx:], ", "))
	if err != nil {
		return nil, errors.Wrap(err, "parsing granularity failed")
	}

	engine.Granularity = granularity

	primary := strings.Trim(params[:idx], "() ")

	primaryKey := strings.Split(primary, ",")

	for i, col := range primaryKey {
		primaryKey[i] = strings.TrimSpace(col)
	}

	engine.PrimaryKey = primaryKey

	return &engine, nil
}

type clickhouseEngine struct {
	Name            string
	PartitioningKey string
	// SamplingKey     string
	PrimaryKey  []string
	Granularity int
}

// ForeignKeyInfo retrieves the foreign keys for a given table name.
func (m *ClickhouseDriver) ForeignKeyInfo(schema, table string) ([]bdb.ForeignKey, error) {
	return nil, nil
}

// TranslateColumnType converts clickhouse database types to Go types, for example
// "String" to "string" and "Int64" to "int64". It returns this parsed data
// as a Column object.
func (m *ClickhouseDriver) TranslateColumnType(c bdb.Column) bdb.Column {
	switch c.DBType {
	case "UInt8":
		if TinyintAsBool {
			c.Type = "bool"
		} else {
			c.Type = "uint8"
		}
	case "UInt16":
		c.Type = "uint16"
	case "UInt32":
		c.Type = "uint32"
	case "UInt64":
		c.Type = "uint64"
	case "Int8":
		c.Type = "int8"
	case "Int16":
		c.Type = "int16"
	case "Int32":
		c.Type = "int32"
	case "Int64":
		c.Type = "int64"
	case "Float32":
		c.Type = "float32"
	case "Float64":
		c.Type = "float64"
	case "Date", "DateTime":
		c.Type = "time.Time"
	case "FixedString":
		c.Type = "types.FixedString"
	case "String":
		c.Type = "string"
	default:
		c.Type = "[]byte"
	}

	return c
}

// RightQuote is the quoting character for the right side of the identifier
func (m *ClickhouseDriver) RightQuote() byte {
	return '`'
}

// LeftQuote is the quoting character for the left side of the identifier
func (m *ClickhouseDriver) LeftQuote() byte {
	return '`'
}

// IndexPlaceholders returns false to indicate Clickhouse doesnt support indexed placeholders
func (m *ClickhouseDriver) IndexPlaceholders() bool {
	return false
}
