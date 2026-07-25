package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v9/internal/infra"
	"github.com/charlesAcmen/livestream-danmaku/v9/internal/repo"
)

func main() {
	room := flag.String("room", "room1", "room id")
	limit := flag.Int("limit", 10, "max rows to return")
	mysqlDSN := flag.String("mysql-dsn", getenv("V9_MYSQL_DSN", infra.DefaultMySQLDSN), "MySQL DSN")
	flag.Parse()

	dbCtx, cancelDB := context.WithTimeout(context.Background(), 5*time.Second)
	db, err := infra.OpenDB(dbCtx, *mysqlDSN)
	cancelDB()
	if err != nil {
		log.Fatalf("[query] init mysql failed: %v", err)
	}
	sqlDB, err := db.DB()
	if err == nil {
		defer sqlDB.Close()
	}

	messageRepo := repo.NewMessageRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messages, err := messageRepo.ListRecentByRoom(ctx, *room, *limit)
	if err != nil {
		log.Fatalf("[query] list messages failed: %v", err)
	}
	count, err := messageRepo.CountByRoom(ctx, *room)
	if err != nil {
		log.Fatalf("[query] count messages failed: %v", err)
	}

	output := map[string]any{
		"room":     *room,
		"count":    count,
		"messages": messages,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		log.Fatalf("[query] encode output failed: %v", err)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
