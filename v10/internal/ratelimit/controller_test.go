package ratelimit

import (
	"testing"
	"time"
)

type manualClock struct {
	now time.Time
}

func (c *manualClock) Now() time.Time {
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func TestConnectionAdmissionLimitsPerIPAndGlobalCount(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	controller := newController(Config{
		MaxConnections:      2,
		MaxConnectionsPerIP: 1,
	}, clock.Now)

	releaseIP1, ok := controller.AcquireConnection("10.0.0.1")
	if !ok {
		t.Fatal("first connection from ip1 was rejected")
	}
	if _, ok := controller.AcquireConnection("10.0.0.1"); ok {
		t.Fatal("second connection from ip1 was accepted past per-IP limit")
	}

	releaseIP2, ok := controller.AcquireConnection("10.0.0.2")
	if !ok {
		t.Fatal("connection from ip2 was rejected before global limit")
	}
	if _, ok := controller.AcquireConnection("10.0.0.3"); ok {
		t.Fatal("third total connection was accepted past global limit")
	}

	releaseIP1()
	releaseIP1() // Release must be idempotent.
	if _, ok := controller.AcquireConnection("10.0.0.1"); !ok {
		t.Fatal("connection was not accepted after capacity was released")
	}
	releaseIP2()

	metrics := controller.Metrics()
	if metrics.RejectedConnectionsPerIP != 1 {
		t.Fatalf("per-IP rejections = %d, want 1", metrics.RejectedConnectionsPerIP)
	}
	if metrics.RejectedConnectionsGlobal != 1 {
		t.Fatalf("global rejections = %d, want 1", metrics.RejectedConnectionsGlobal)
	}
}

func TestDanmakuUserTokenBucketAllowsBurstThenRefills(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	controller := newController(Config{
		DanmakuPerUser: Rate{PerSecond: 2, Burst: 2},
		DanmakuPerRoom: Rate{PerSecond: 100, Burst: 100},
	}, clock.Now)

	for i := 0; i < 2; i++ {
		if ok, reason := controller.AllowDanmaku("room-1", "user-1"); !ok || reason != ReasonNone {
			t.Fatalf("burst request %d = (%v, %q), want allowed", i+1, ok, reason)
		}
	}
	if ok, reason := controller.AllowDanmaku("room-1", "user-1"); ok || reason != ReasonUser {
		t.Fatalf("third request = (%v, %q), want user rejection", ok, reason)
	}

	clock.Advance(500 * time.Millisecond)
	if ok, reason := controller.AllowDanmaku("room-1", "user-1"); !ok || reason != ReasonNone {
		t.Fatalf("request after refill = (%v, %q), want allowed", ok, reason)
	}
}

func TestDanmakuRoomBudgetIsSharedByDifferentUsers(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	controller := newController(Config{
		DanmakuPerUser: Rate{PerSecond: 100, Burst: 100},
		DanmakuPerRoom: Rate{PerSecond: 1, Burst: 2},
	}, clock.Now)

	if ok, _ := controller.AllowDanmaku("hot-room", "user-1"); !ok {
		t.Fatal("first room request was rejected")
	}
	if ok, _ := controller.AllowDanmaku("hot-room", "user-2"); !ok {
		t.Fatal("second room request was rejected")
	}
	if ok, reason := controller.AllowDanmaku("hot-room", "user-3"); ok || reason != ReasonRoom {
		t.Fatalf("third room request = (%v, %q), want room rejection", ok, reason)
	}

	if ok, reason := controller.AllowDanmaku("other-room", "user-3"); !ok || reason != ReasonNone {
		t.Fatalf("other room request = (%v, %q), want allowed", ok, reason)
	}
}

func TestLikeAndDanmakuUseIndependentBudgets(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	controller := newController(Config{
		DanmakuPerUser: Rate{PerSecond: 1, Burst: 1},
		DanmakuPerRoom: Rate{PerSecond: 10, Burst: 10},
		LikePerUser:    Rate{PerSecond: 1, Burst: 1},
		LikePerRoom:    Rate{PerSecond: 10, Burst: 10},
	}, clock.Now)

	if ok, _ := controller.AllowDanmaku("room-1", "user-1"); !ok {
		t.Fatal("first danmaku was rejected")
	}
	if ok, _ := controller.AllowLike("room-1", "user-1"); !ok {
		t.Fatal("like incorrectly consumed the danmaku budget")
	}
	if ok, reason := controller.AllowLike("room-1", "user-1"); ok || reason != ReasonUser {
		t.Fatalf("second like = (%v, %q), want user rejection", ok, reason)
	}

	metrics := controller.Metrics()
	if metrics.DanmakuAccepted != 1 || metrics.LikeAccepted != 1 || metrics.LikeRejectedUser != 1 {
		t.Fatalf("unexpected traffic metrics: %+v", metrics)
	}
}
