import (
	"bytes"
	"database/sql"
	"fmt"
	"net/url"
	"os/exec"

	"github.com/spf13/viper"
	"github.com/volatiletech/sqlboiler/bdb/drivers"
	"github.com/volatiletech/sqlboiler/randomize"
)

type clickhouseTester struct {
	dbConn *sql.DB

	database   string
	host       string
	username   string
	password   string
	port       int
	secure     bool
	skipVerify bool

	testDBName string
}

func init() {
	dbMain = &clickhouseTester{}
}

func (m *clickhouseTester) setup() error {
	var err error

	m.database = viper.GetString("clickhouse.database")
	m.host = viper.GetString("clickhouse.host")
	m.username = viper.GetString("clickhouse.username")
	m.password = viper.GetString("clickhouse.password")
	m.port = viper.GetInt("clickhouse.port")
	m.secure = viper.GetBool("clickhouse.secure")
	// Create a randomized db name.
	m.testDBName = randomize.StableDBName(m.database)

	if err = m.dropTestDB(); err != nil {
		return err
	}
	if err = m.createTestDB(); err != nil {
		return err
	}

	return nil
}

func (m *clickhouseTester) createTestDB() error {
	sql := fmt.Sprintf("create database %s", m.testDBName)
	return m.runQuery(sql)
}

func (m *clickhouseTester) dropTestDB() error {
	sql := fmt.Sprintf("drop database if exists %s", m.testDBName)
	return m.runQuery(sql)
}

func (m *clickhouseTester) teardown() error {
	if m.dbConn != nil {
		m.dbConn.Close()
	}

	if err := m.dropTestDB(); err != nil {
		return err
	}

	return nil
}

func (m *clickhouseTester) runQuery(q string) error {
	u, err := url.Parse("http://localhost:8123/")
	if err != nil {
		return fmt.Errorf("parsing url failed: %s", err)
	}

	query := u.Query()
	query.Set("query", q)

	u.RawQuery = query.Encode()

	cmd := exec.Command("wget", "--method", "POST", "--content-on-error", "-qO-", u.String())

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		fmt.Println("failed running:", q)
		fmt.Println(stdout.String())
		fmt.Println(stderr.String())
		return err
	}

	return nil
}

func (m *clickhouseTester) conn() (*sql.DB, error) {
	if m.dbConn != nil {
		return m.dbConn, nil
	}

	var err error
	cfg := drivers.ClickhouseDriverConfig{
		Username: m.username,
		Password: m.password,
		Database: m.testDBName,
		Host:     m.host,
		Port:     m.port,
		Secure:   m.secure,
	}
	m.dbConn, err = sql.Open("clickhouse", drivers.ClickhouseBuildQueryString(cfg))
	if err != nil {
		return nil, err
	}

	return m.dbConn, nil
}
