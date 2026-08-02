package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v10/internal/infra"
)

func main() {
	mysqlDSN := flag.String("mysql-dsn", getenv("V10_MYSQL_DSN", infra.DefaultMySQLDSN), "MySQL DSN")
	seedUserID := flag.String("seed-user-id", getenv("V10_SEED_USER_ID", "user-demo"), "local learning user id")
	seedUsername := flag.String("seed-username", getenv("V10_SEED_USERNAME", "demo"), "local learning username")
	seedPassword := flag.String("seed-password", getenv("V10_SEED_PASSWORD", "demo123"), "local learning password")
	seedRole := flag.String("seed-role", getenv("V10_SEED_ROLE", "viewer"), "local learning user role")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := infra.OpenDB(ctx, *mysqlDSN)
	if err != nil {
		log.Fatalf("[migrate] open mysql failed: %v", err)
	}
	sqlDB, err := db.DB()
	if err == nil {
		defer sqlDB.Close()
	}

	if err := infra.Migrate(db); err != nil {
		log.Fatalf("[migrate] migration failed: %v", err)
	}
	if strings.TrimSpace(*seedUsername) != "" {
		if err := infra.SeedUser(ctx, db, *seedUserID, *seedUsername, *seedPassword, *seedRole); err != nil {
			log.Fatalf("[migrate] seed user failed: %v", err)
		}
		log.Printf("[migrate] local login user is ready username=%s", *seedUsername)
	}
	log.Printf("[migrate] V10 schema is ready")
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
