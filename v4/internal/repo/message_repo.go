package repo

import (
	"context"

	"github.com/charlesAcmen/livestream-danmaku/v4/internal/model"
	"gorm.io/gorm"
)

type MessageRepo struct {
	db *gorm.DB
}

func NewMessageRepo(db *gorm.DB) *MessageRepo {
	return &MessageRepo{db: db}
}

func (r *MessageRepo) CreateInBatches(ctx context.Context, messages []*model.Danmaku) error {
	if len(messages) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).CreateInBatches(messages, len(messages)).Error
}

func (r *MessageRepo) ListRecentByRoom(ctx context.Context, roomID string, limit int) ([]model.Danmaku, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	var messages []model.Danmaku
	err := r.db.WithContext(ctx).
		Where("room_id = ?", roomID).
		Order("send_time DESC").
		Limit(limit).
		Find(&messages).Error
	return messages, err
}

func (r *MessageRepo) CountByRoom(ctx context.Context, roomID string) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&model.Danmaku{}).
		Where("room_id = ?", roomID).
		Count(&count).Error
	return count, err
}
