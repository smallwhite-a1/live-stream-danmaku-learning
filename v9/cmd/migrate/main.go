package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v9/internal/infra"
)

func main() {
	mysqlDSN := flag.String("mysql-dsn", getenv("V9_MYSQL_DSN", infra.DefaultMySQLDSN), "MySQL DSN")
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
	log.Printf("[migrate] V9 schema is ready")
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
