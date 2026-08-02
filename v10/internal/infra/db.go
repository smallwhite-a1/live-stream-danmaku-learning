package infra

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const DefaultMySQLDSN = "root:root@tcp(127.0.0.1:3313)/danmaku_v10?charset=utf8mb4&parseTime=True&loc=Local"

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
	if err := db.AutoMigrate(&model.User{}); err != nil {
		return fmt.Errorf("migrate user table: %w", err)
	}
	return nil
}

// SeedUser creates a local learning account only when the username does not
// exist. Existing passwords are never overwritten by a migration rerun.
func SeedUser(ctx context.Context, db *gorm.DB, userID, username, password, role string) error {
	userID = strings.TrimSpace(userID)
	username = strings.TrimSpace(username)
	if userID == "" || username == "" || password == "" {
		return fmt.Errorf("seed user id, username, and password are required")
	}
	if role == "" {
		role = "viewer"
	}

	var existing model.User
	err := db.WithContext(ctx).Where("username = ?", username).First(&existing).Error
	if err == nil {
		return nil
	}
	if err != gorm.ErrRecordNotFound {
		return fmt.Errorf("find seed user: %w", err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash seed password: %w", err)
	}
	return db.WithContext(ctx).Create(&model.User{
		ID:           userID,
		Username:     username,
		PasswordHash: string(hash),
		Role:         role,
		Status:       model.UserStatusActive,
	}).Error
}
