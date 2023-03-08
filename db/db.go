package db

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

const DealsDBName = "boost.db"
const LogsDBName = "boost.logs.db"

var ErrNotFound = errors.New("not found")

type Scannable interface {
	Scan(dest ...interface{}) error
}

func SqlDB(dbPath, driverName string) (*sql.DB, *sqlite3.SQLiteConn, error) {
	var sqlite3conn *sqlite3.SQLiteConn
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			sqlite3conn = conn
			return nil
		},
	})
	db, err := sql.Open(driverName, "file:"+dbPath)
	if err == nil {
		// fixes error "database is locked", caused by concurrent access from deal goroutines to a single sqlite3 db connection
		// see: https://github.com/mattn/go-sqlite3#:~:text=Error%3A%20database%20is%20locked
		// see: https://github.com/filecoin-project/boost/pull/657
		db.SetMaxOpenConns(1)
	}

	// Open the connection to ensure sqlite3conn is not returned as nil. sql.Open does not starts a connection
	if err := db.Ping(); err != nil {
		return &sql.DB{}, &sqlite3.SQLiteConn{}, err
	}

	return db, sqlite3conn, err
}

//go:embed create_main_db.sql
var createMainDBSQL string

//go:embed create_logs_db.sql
var createLogsDBSQL string

func CreateAllBoostTables(ctx context.Context, mainDB *sql.DB, logsDB *sql.DB) error {
	if _, err := mainDB.ExecContext(ctx, createMainDBSQL); err != nil {
		return fmt.Errorf("failed to create tables in main DB: %w", err)
	}

	if _, err := logsDB.ExecContext(ctx, createLogsDBSQL); err != nil {
		return fmt.Errorf("failed to create tables in logs DB: %w", err)
	}
	return nil
}

func CreateTestTmpDB(t *testing.T) *sql.DB {
	f, err := ioutil.TempFile(t.TempDir(), "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	d, _, err := SqlDB(f.Name(), uuid.NewString())
	require.NoError(t, err)
	return d
}
