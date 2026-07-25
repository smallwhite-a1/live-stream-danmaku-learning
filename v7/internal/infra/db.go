package infra

import (
	"fmt"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v7/internal/model"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const DefaultMySQLDSN = "root:root@tcp(127.0.0.1:3310)/danmaku_v7?charset=utf8mb4&parseTime=True&loc=Local"

func InitDB(dsn string) (*gorm.DB, error) {
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
	sqlDB.SetMaxOpenConns(20)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := db.AutoMigrate(&model.Danmaku{}); err != nil {
		return nil, fmt.Errorf("auto migrate: %w", err)
	}

	return db, nil
}
