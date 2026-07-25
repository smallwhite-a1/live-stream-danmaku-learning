package infra

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultRedisAddr = "127.0.0.1:6380"
	KeyRoomPubSub    = "v6:room:%s:pubsub"
	RedisTimeout     = 2 * time.Second
)

func InitRedisClient(addr string) *redis.Client {
	if addr == "" {
		addr = DefaultRedisAddr
	}
	return redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0,
	})
}

func PingRedis(ctx context.Context, client *redis.Client) error {
	pingCtx, cancel := context.WithTimeout(ctx, RedisTimeout)
	defer cancel()
	return client.Ping(pingCtx).Err()
}

func RoomChannel(roomID string) string {
	return fmt.Sprintf(KeyRoomPubSub, roomID)
}

func PublishToRoom(ctx context.Context, client *redis.Client, roomID string, payload []byte) error {
	publishCtx, cancel := context.WithTimeout(ctx, RedisTimeout)
	defer cancel()
	return client.Publish(publishCtx, RoomChannel(roomID), payload).Err()
}
