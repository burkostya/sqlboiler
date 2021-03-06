package boilingcore

// Config for the running of the commands
type Config struct {
	DriverName       string
	Schema           string
	PkgName          string
	OutFolder        string
	BaseDir          string
	WhitelistTables  []string
	BlacklistTables  []string
	Tags             []string
	Replacements     []string
	Debug            bool
	NoTests          bool
	NoHooks          bool
	NoAutoTimestamps bool
	Wipe             bool
	StructTagCasing  string

	Postgres   PostgresConfig
	MySQL      MySQLConfig
	MSSQL      MSSQLConfig
	Clickhouse ClickhouseConfig
}

// PostgresConfig configures a postgres database
type PostgresConfig struct {
	User    string
	Pass    string
	Host    string
	Port    int
	DBName  string
	SSLMode string
}

// MySQLConfig configures a mysql database
type MySQLConfig struct {
	User    string
	Pass    string
	Host    string
	Port    int
	DBName  string
	SSLMode string
}

// MSSQLConfig configures a mysql database
type MSSQLConfig struct {
	User    string
	Pass    string
	Host    string
	Port    int
	DBName  string
	SSLMode string
}

// ClickhouseConfig configures a clickhouse database
type ClickhouseConfig struct {
	Username               string
	Password               string
	Database               string
	Host                   string
	Port                   int
	ReadTimeout            int
	WriteTimeout           int
	NoDelay                bool
	AltHosts               []string
	ConnectionOpenStrategy string
	BlockSize              int
	Debug                  bool
	Secure                 bool
	SkipVerify             bool
}
