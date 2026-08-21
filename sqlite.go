package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type sqliteDB struct {
	pool *sqlitex.Pool
}

func openDB(path string) (*sqliteDB, error) {
	db, err := openSQLitePool(path, sqlite.OpenReadWrite|sqlite.OpenCreate|sqlite.OpenWAL|sqlite.OpenURI, 2, true)
	if err != nil {
		return nil, err
	}
	if err := initDB(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func openWritableExistingDB(path string) (*sqliteDB, error) {
	db, err := openSQLitePool(path, sqlite.OpenReadWrite|sqlite.OpenWAL|sqlite.OpenURI, 1, true)
	if err != nil {
		return nil, err
	}
	if err := validateDBVersion(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func openExistingDB(path string) (*sqliteDB, error) {
	db, err := openSQLitePool(path, sqlite.OpenReadOnly|sqlite.OpenURI, 1, false)
	if err != nil {
		return nil, err
	}
	if err := validateDBVersion(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func validateDBVersion(db *sqliteDB) error {
	var version int64
	err := db.withConn(context.Background(), func(conn *sqlite.Conn) error {
		var err error
		version, err = queryInt64(conn, `PRAGMA user_version`, nil)
		return err
	})
	if err != nil {
		return fmt.Errorf("read catalog version: %w", err)
	}
	if version != cacheSchemaVersion {
		return fmt.Errorf("unsupported catalog version %d, want %d", version, cacheSchemaVersion)
	}
	return nil
}

func openSQLitePool(path string, flags sqlite.OpenFlags, size int, writable bool) (*sqliteDB, error) {
	dsn := "file:" + url.PathEscape(filepath.ToSlash(path))
	pool, err := sqlitex.NewPool(dsn, sqlitex.PoolOptions{
		Flags:    flags,
		PoolSize: size,
		PrepareConn: func(conn *sqlite.Conn) error {
			conn.SetBusyTimeout(5 * time.Second)
			if !writable {
				return nil
			}
			if err := sqlitex.Execute(conn, `PRAGMA synchronous = NORMAL`, nil); err != nil {
				return err
			}
			return sqlitex.Execute(conn, `PRAGMA foreign_keys = ON`, nil)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	return &sqliteDB{pool: pool}, nil
}

func (db *sqliteDB) Close() error {
	return db.pool.Close()
}

func (db *sqliteDB) withConn(ctx context.Context, fn func(*sqlite.Conn) error) error {
	conn, err := db.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer db.pool.Put(conn)
	return fn(conn)
}

func (db *sqliteDB) withTx(ctx context.Context, fn func(*sqlite.Conn) error) error {
	return db.withConn(ctx, func(conn *sqlite.Conn) error {
		// All current transactions write. Acquire the write lock before taking a
		// read snapshot so concurrent helpers wait instead of failing with
		// SQLITE_BUSY_SNAPSHOT when upgrading a deferred transaction.
		if err := sqlitex.Execute(conn, `BEGIN IMMEDIATE`, nil); err != nil {
			return err
		}
		if err := fn(conn); err != nil {
			conn.SetInterrupt(nil)
			rollbackErr := sqlitex.Execute(conn, `ROLLBACK`, nil)
			return errors.Join(err, rollbackErr)
		}
		if err := sqlitex.Execute(conn, `COMMIT`, nil); err != nil {
			conn.SetInterrupt(nil)
			rollbackErr := sqlitex.Execute(conn, `ROLLBACK`, nil)
			return errors.Join(err, rollbackErr)
		}
		return nil
	})
}

func execute(conn *sqlite.Conn, query string, args ...any) error {
	return sqlitex.Execute(conn, query, &sqlitex.ExecOptions{Args: args})
}

func queryInt64(conn *sqlite.Conn, query string, args []any) (int64, error) {
	var value int64
	found := false
	err := sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		Args: args,
		ResultFunc: func(stmt *sqlite.Stmt) error {
			found = true
			value = stmt.ColumnInt64(0)
			return nil
		},
	})
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, errors.New("query returned no rows")
	}
	return value, nil
}

func execPrepared(conn *sqlite.Conn, query string, bind func(*sqlite.Stmt)) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}
	bind(stmt)
	if _, err := stmt.Step(); err != nil {
		_ = stmt.Reset()
		return err
	}
	return stmt.Reset()
}

func queryPrepared(conn *sqlite.Conn, query string, bind func(*sqlite.Stmt), row func(*sqlite.Stmt)) (bool, error) {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return false, err
	}
	bind(stmt)
	found, err := stmt.Step()
	if err != nil {
		_ = stmt.Reset()
		return false, err
	}
	if found {
		row(stmt)
	}
	if err := stmt.Reset(); err != nil {
		return false, err
	}
	return found, nil
}
