package o

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const Mysql = "mysql"

// Use Map to manage multiple database connections
var GormDBs = make(map[string]*gorm.DB)

// DBConfig database configuration structure
type DBConfig struct {
	Host         string
	Port         string
	User         string
	Password     string
	DBName       string
	Options      map[string]string
	MaxIdleConns int
	MaxOpenConns int
	MaxLifetime  time.Duration
	MaxIdleTime  time.Duration
}

// InitGormDB initialize GORM database connection default
func InitGormDB(config DBConfig) error {
	return InitDBWithMap(map[string]DBConfig{Mysql: config})
}

// GetDBDefault if using InitGormDB, get default database connection
func GetDBDefault() *gorm.DB {
	if db, exists := GormDBs[Mysql]; exists && db != nil {
		return db
	}
	return nil
}

// GetDB get database connection by name
func GetDB(name string) *gorm.DB {
	if name == "" {
		return nil
	}
	if db, exists := GormDBs[name]; exists && db != nil {
		return db
	}
	return nil
}

// InitDBWithMap initialize database connections using Map method
func InitDBWithMap(configs map[string]DBConfig) error {
	for name, config := range configs {
		// Validate required configuration parameters
		if config.Host == "" || config.Port == "" || config.User == "" || config.DBName == "" {
			return fmt.Errorf("invalid configuration for database %s: missing required fields", name)
		}

		dsn := buildDSN(config.Host, config.Port, config.User, config.Password, config.DBName, config.Options)

		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			return fmt.Errorf("failed to connect to %s database: %v", name, err)
		}

		sqlDB, err := db.DB()
		if err != nil {
			return fmt.Errorf("failed to get underlying sql.DB for %s: %v", name, err)
		}

		// Configure connection pool
		if config.MaxIdleConns > 0 {
			sqlDB.SetMaxIdleConns(config.MaxIdleConns) // Maximum idle connections
		}
		if config.MaxOpenConns > 0 {
			sqlDB.SetMaxOpenConns(config.MaxOpenConns) // Maximum open connections
		}
		if config.MaxLifetime > 0 {
			sqlDB.SetConnMaxLifetime(config.MaxLifetime) // Maximum connection lifetime
		}
		if config.MaxIdleTime > 0 {
			sqlDB.SetConnMaxIdleTime(config.MaxIdleTime) // Maximum idle connection lifetime
		}

		GormDBs[name] = db
	}

	return nil
}

// GetDBStats get specified database connection pool statistics
func GetDBStats(db *gorm.DB) map[string]interface{} {
	if db == nil {
		return nil
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil
	}

	stats := sqlDB.Stats()
	return map[string]interface{}{
		"max_open_connections": stats.MaxOpenConnections,
		"open_connections":     stats.OpenConnections,
		"in_use":               stats.InUse,
		"idle":                 stats.Idle,
		"wait_count":           stats.WaitCount,
		"wait_duration":        stats.WaitDuration,
		"max_idle_closed":      stats.MaxIdleClosed,
		"max_lifetime_closed":  stats.MaxLifetimeClosed,
	}
}

// IsDBValid check if database connection is valid
func IsDBValid(db *gorm.DB) bool {
	if db == nil {
		return false
	}

	sqlDB, err := db.DB()
	if err != nil {
		return false
	}

	// Try to ping the database
	if err := sqlDB.Ping(); err != nil {
		return false
	}

	return true
}

// GetDBStatsByName get database connection pool statistics by name
func GetDBStatsByName(dbName string) map[string]interface{} {
	db := GetDB(dbName)
	return GetDBStats(db)
}

// CloseAllDBs close all database connections
func CloseAllDBs() error {
	var lastErr error
	for name, db := range GormDBs {
		if db != nil {
			sqlDB, err := db.DB()
			if err != nil {
				lastErr = fmt.Errorf("failed to get sql.DB for %s: %v", name, err)
				continue
			}

			if err = sqlDB.Close(); err != nil {
				lastErr = fmt.Errorf("failed to close %s database: %v", name, err)
			}
		}
	}

	// Clear connection mapping
	GormDBs = make(map[string]*gorm.DB)
	return lastErr
}

// BeginTx start transaction
func BeginTx(dbName string) (*gorm.DB, error) {
	db := GetDB(dbName)
	if db == nil {
		return nil, fmt.Errorf("database %s not found", dbName)
	}

	tx := db.Begin()
	return tx, tx.Error
}

// BeginTransactionDefault start default database transaction
func BeginTxDefault() (*gorm.DB, error) {
	return BeginTx(Mysql)
}

// CommitTx commit transaction, automatically rollback if err is not nil
func CommitTx(tx *gorm.DB, err *error) {
	if tx == nil {
		if *err == nil {
			*err = fmt.Errorf("transaction is nil")
		}
		return
	}

	if *err != nil {
		// Rollback transaction when there's an error
		if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
			*err = fmt.Errorf("rollback failed: %v, original error: %v", rollbackErr, *err)
		}
		return
	}

	// Commit transaction when there's no error
	if commitErr := tx.Commit().Error; commitErr != nil {
		*err = fmt.Errorf("commit failed: %v", commitErr)
		return
	}
}

// buildDSN build DSN string
func buildDSN(host, port, user, password, dbname string, options map[string]string) string {
	// Basic DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)

	// Default options
	defaultOptions := map[string]string{
		"charset":   "utf8mb4",
		"parseTime": "True",
		"loc":       "Local",
	}

	// Merge user options with default options
	for k, v := range options {
		defaultOptions[k] = v
	}

	// Build query parameters
	var queryParams []string
	for k, v := range defaultOptions {
		queryParams = append(queryParams, fmt.Sprintf("%s=%s", k, v))
	}

	if len(queryParams) > 0 {
		dsn += "?" + strings.Join(queryParams, "&")
	}

	return dsn
}
