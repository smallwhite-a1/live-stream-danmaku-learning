package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	shardCount           = 64
	defaultBucketIdleTTL = 10 * time.Minute
)

type Reason string

const (
	ReasonNone Reason = ""
	ReasonUser Reason = "user"
	ReasonRoom Reason = "room"
)

type Rate struct {
	PerSecond float64
	Burst     int
}

type Config struct {
	MaxConnections      int
	MaxConnectionsPerIP int
	DanmakuPerUser      Rate
	DanmakuPerRoom      Rate
	LikePerUser         Rate
	LikePerRoom         Rate
	BucketIdleTTL       time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxConnections:      5000,
		MaxConnectionsPerIP: 1000,
		DanmakuPerUser:      Rate{PerSecond: 5, Burst: 10},
		DanmakuPerRoom:      Rate{PerSecond: 500, Burst: 1000},
		LikePerUser:         Rate{PerSecond: 20, Burst: 40},
		LikePerRoom:         Rate{PerSecond: 5000, Burst: 10000},
		BucketIdleTTL:       defaultBucketIdleTTL,
	}
}

type Metrics struct {
	CurrentConnections        int64  `json:"current_connections"`
	AcceptedConnections       uint64 `json:"accepted_connections"`
	RejectedConnectionsGlobal uint64 `json:"rejected_connections_global"`
	RejectedConnectionsPerIP  uint64 `json:"rejected_connections_per_ip"`
	DanmakuAccepted           uint64 `json:"danmaku_accepted"`
	DanmakuRejectedUser       uint64 `json:"danmaku_rejected_user"`
	DanmakuRejectedRoom       uint64 `json:"danmaku_rejected_room"`
	LikeAccepted              uint64 `json:"like_accepted"`
	LikeRejectedUser          uint64 `json:"like_rejected_user"`
	LikeRejectedRoom          uint64 `json:"like_rejected_room"`
}

type Controller struct {
	config Config
	now    func() time.Time

	connectionsMu sync.Mutex
	connections   int64
	connectionsIP map[string]int

	danmakuUsers *keyedLimiter
	danmakuRooms *keyedLimiter
	likeUsers    *keyedLimiter
	likeRooms    *keyedLimiter

	acceptedConnections       atomic.Uint64
	rejectedConnectionsGlobal atomic.Uint64
	rejectedConnectionsPerIP  atomic.Uint64
	danmakuAccepted           atomic.Uint64
	danmakuRejectedUser       atomic.Uint64
	danmakuRejectedRoom       atomic.Uint64
	likeAccepted              atomic.Uint64
	likeRejectedUser          atomic.Uint64
	likeRejectedRoom          atomic.Uint64
}

func New(config Config) *Controller {
	return newController(config, time.Now)
}

func newController(config Config, now func() time.Time) *Controller {
	if config.BucketIdleTTL <= 0 {
		config.BucketIdleTTL = defaultBucketIdleTTL
	}
	if now == nil {
		now = time.Now
	}

	return &Controller{
		config:        config,
		now:           now,
		connectionsIP: make(map[string]int),
		danmakuUsers:  newKeyedLimiter(config.DanmakuPerUser, config.BucketIdleTTL, now),
		danmakuRooms:  newKeyedLimiter(config.DanmakuPerRoom, config.BucketIdleTTL, now),
		likeUsers:     newKeyedLimiter(config.LikePerUser, config.BucketIdleTTL, now),
		likeRooms:     newKeyedLimiter(config.LikePerRoom, config.BucketIdleTTL, now),
	}
}

func (c *Controller) AcquireConnection(ip string) (func(), bool) {
	c.connectionsMu.Lock()
	if c.config.MaxConnections > 0 && c.connections >= int64(c.config.MaxConnections) {
		c.connectionsMu.Unlock()
		c.rejectedConnectionsGlobal.Add(1)
		return func() {}, false
	}
	if c.config.MaxConnectionsPerIP > 0 && c.connectionsIP[ip] >= c.config.MaxConnectionsPerIP {
		c.connectionsMu.Unlock()
		c.rejectedConnectionsPerIP.Add(1)
		return func() {}, false
	}

	c.connections++
	c.connectionsIP[ip]++
	c.connectionsMu.Unlock()
	c.acceptedConnections.Add(1)

	var once sync.Once
	release := func() {
		once.Do(func() {
			c.connectionsMu.Lock()
			c.connections--
			c.connectionsIP[ip]--
			if c.connectionsIP[ip] == 0 {
				delete(c.connectionsIP, ip)
			}
			c.connectionsMu.Unlock()
		})
	}
	return release, true
}

func (c *Controller) AllowDanmaku(roomID, userID string) (bool, Reason) {
	userKey := roomID + "\x00" + userID
	if !c.danmakuUsers.Allow(userKey) {
		c.danmakuRejectedUser.Add(1)
		return false, ReasonUser
	}
	if !c.danmakuRooms.Allow(roomID) {
		c.danmakuRejectedRoom.Add(1)
		return false, ReasonRoom
	}

	c.danmakuAccepted.Add(1)
	return true, ReasonNone
}

func (c *Controller) AllowLike(roomID, userID string) (bool, Reason) {
	userKey := roomID + "\x00" + userID
	if !c.likeUsers.Allow(userKey) {
		c.likeRejectedUser.Add(1)
		return false, ReasonUser
	}
	if !c.likeRooms.Allow(roomID) {
		c.likeRejectedRoom.Add(1)
		return false, ReasonRoom
	}

	c.likeAccepted.Add(1)
	return true, ReasonNone
}

func (c *Controller) Metrics() Metrics {
	c.connectionsMu.Lock()
	connections := c.connections
	c.connectionsMu.Unlock()

	return Metrics{
		CurrentConnections:        connections,
		AcceptedConnections:       c.acceptedConnections.Load(),
		RejectedConnectionsGlobal: c.rejectedConnectionsGlobal.Load(),
		RejectedConnectionsPerIP:  c.rejectedConnectionsPerIP.Load(),
		DanmakuAccepted:           c.danmakuAccepted.Load(),
		DanmakuRejectedUser:       c.danmakuRejectedUser.Load(),
		DanmakuRejectedRoom:       c.danmakuRejectedRoom.Load(),
		LikeAccepted:              c.likeAccepted.Load(),
		LikeRejectedUser:          c.likeRejectedUser.Load(),
		LikeRejectedRoom:          c.likeRejectedRoom.Load(),
	}
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
	lastSeen   time.Time
}

type bucketShard struct {
	mu          sync.Mutex
	buckets     map[string]*tokenBucket
	lastCleanup time.Time
}

type keyedLimiter struct {
	rate    Rate
	idleTTL time.Duration
	now     func() time.Time
	shards  [shardCount]bucketShard
}

func newKeyedLimiter(rate Rate, idleTTL time.Duration, now func() time.Time) *keyedLimiter {
	limiter := &keyedLimiter{rate: rate, idleTTL: idleTTL, now: now}
	for i := range limiter.shards {
		limiter.shards[i].buckets = make(map[string]*tokenBucket)
	}
	return limiter
}

func (l *keyedLimiter) Allow(key string) bool {
	if l.rate.PerSecond <= 0 || l.rate.Burst <= 0 {
		return true
	}

	now := l.now()
	shard := &l.shards[hashKey(key)%shardCount]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.lastCleanup.IsZero() || now.Sub(shard.lastCleanup) >= l.idleTTL {
		for bucketKey, bucket := range shard.buckets {
			if now.Sub(bucket.lastSeen) >= l.idleTTL {
				delete(shard.buckets, bucketKey)
			}
		}
		shard.lastCleanup = now
	}

	bucket, ok := shard.buckets[key]
	if !ok {
		bucket = &tokenBucket{
			tokens:     float64(l.rate.Burst),
			lastRefill: now,
			lastSeen:   now,
		}
		shard.buckets[key] = bucket
	}

	elapsed := now.Sub(bucket.lastRefill).Seconds()
	if elapsed > 0 {
		bucket.tokens += elapsed * l.rate.PerSecond
		if bucket.tokens > float64(l.rate.Burst) {
			bucket.tokens = float64(l.rate.Burst)
		}
		bucket.lastRefill = now
	}
	bucket.lastSeen = now

	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

func hashKey(key string) int {
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	hash := offset32
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return int(hash & (shardCount - 1))
}
