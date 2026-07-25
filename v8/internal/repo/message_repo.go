package repo

import (
	"context"

	"github.com/charlesAcmen/livestream-danmaku/v8/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type MessageRepo struct {
	db *gorm.DB
}

func NewMessageRepo(db *gorm.DB) *MessageRepo {
	return &MessageRepo{db: db}
}

// CreateIdempotent inserts a batch while treating MessageID conflicts as an
// already completed retry. RowsAffected is the number of physically new rows.
func (r *MessageRepo) CreateIdempotent(ctx context.Context, messages []*model.Danmaku) (int64, error) {
	if len(messages) == 0 {
		return 0, nil
	}

	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "message_id"}},
			DoNothing: true,
		}).
		CreateInBatches(messages, len(messages))
	return result.RowsAffected, result.Error
}

func (r *MessageRepo) ListRecentByRoom(ctx context.Context, roomID string, limit int) ([]model.Danmaku, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	var messages []model.Danmaku
	err := r.db.WithContext(ctx).
		Where("room_id = ?", roomID).
		Order("send_time DESC, message_id DESC").
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
