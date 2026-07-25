package infra

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultRedisAddr   = "127.0.0.1:6383"
	KeyRoomPubSub      = "v9:room:%s:pubsub"
	KeyRoomLikes       = "v9:room:%s:likes"
	KeyRoomServerCount = "v9:room:%s:online:%s"
	KeyRoomServers     = "v9:room:%s:servers"
	RedisTimeout       = 2 * time.Second
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

func IncrRoomLikes(ctx context.Context, client *redis.Client, roomID string, count uint64) error {
	redisCtx, cancel := context.WithTimeout(ctx, RedisTimeout)
	defer cancel()
	return client.IncrBy(redisCtx, fmt.Sprintf(KeyRoomLikes, roomID), int64(count)).Err()
}

func GetRoomLikes(ctx context.Context, client *redis.Client, roomID string) (uint64, error) {
	redisCtx, cancel := context.WithTimeout(ctx, RedisTimeout)
	defer cancel()

	value, err := client.Get(redisCtx, fmt.Sprintf(KeyRoomLikes, roomID)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	count, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func UpdateServerOnline(ctx context.Context, client *redis.Client, roomID string, serverID string, count int, ttl time.Duration) error {
	redisCtx, cancel := context.WithTimeout(ctx, RedisTimeout)
	defer cancel()

	countKey := fmt.Sprintf(KeyRoomServerCount, roomID, serverID)
	serversKey := fmt.Sprintf(KeyRoomServers, roomID)

	pipe := client.Pipeline()
	pipe.Set(redisCtx, countKey, count, ttl)
	pipe.SAdd(redisCtx, serversKey, serverID)
	_, err := pipe.Exec(redisCtx)
	return err
}

func GetTotalOnline(ctx context.Context, client *redis.Client, roomID string) (uint64, error) {
	redisCtx, cancel := context.WithTimeout(ctx, RedisTimeout)
	defer cancel()

	serversKey := fmt.Sprintf(KeyRoomServers, roomID)
	serverIDs, err := client.SMembers(redisCtx, serversKey).Result()
	if err != nil || len(serverIDs) == 0 {
		return 0, err
	}

	keys := make([]string, 0, len(serverIDs))
	for _, serverID := range serverIDs {
		keys = append(keys, fmt.Sprintf(KeyRoomServerCount, roomID, serverID))
	}

	values, err := client.MGet(redisCtx, keys...).Result()
	if err != nil {
		return 0, err
	}

	var total uint64
	for i, value := range values {
		if value == nil {
			_ = client.SRem(redisCtx, serversKey, serverIDs[i]).Err()
			continue
		}
		asString, ok := value.(string)
		if !ok {
			continue
		}
		count, err := strconv.ParseUint(asString, 10, 64)
		if err == nil {
			total += count
		}
	}

	return total, nil
}
