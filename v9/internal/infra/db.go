package infra

import (
	"context"
	"fmt"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v9/internal/model"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const DefaultMySQLDSN = "root:root@tcp(127.0.0.1:3312)/danmaku_v9?charset=utf8mb4&parseTime=True&loc=Local"

// OpenDB only opens and verifies the connection. Schema changes belong to the
// separate cmd/migrate process so application restarts never mutate tables.
func OpenDB(ctx context.Context, dsn string) (*gorm.DB, error) {
	if dsn == "" {
		dsn = DefaultMySQLDSN
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql db: %w", err)
	}
	sqlDB.SetMaxOpenConns(40)
	sqlDB.SetMaxIdleConns(20)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)

	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping mysql: %w", err)
	}

	return db, nil
}

func Migrate(db *gorm.DB) error {
	if err := db.AutoMigrate(&model.Danmaku{}); err != nil {
		return fmt.Errorf("migrate danmaku table: %w", err)
	}
	return nil
}
