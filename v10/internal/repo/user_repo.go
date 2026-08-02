package repo

import (
	"context"

	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
	"gorm.io/gorm"
)

type UserRepo struct {
	db *gorm.DB
}

func NewUserRepo(db *gorm.DB) *UserRepo {
	return &UserRepo{db: db}
}

func (r *UserRepo) FindByUsername(ctx context.Context, username string) (*model.User, error) {
	var user model.User
	if err := r.db.WithContext(ctx).Where("username = ?", username).First(&user).Error; err != nil {
		return nil, err
	}
	return &user, nil
}

func (r *UserRepo) FindByID(ctx context.Context, userID string) (*model.User, error) {
	var user model.User
	if err := r.db.WithContext(ctx).Where("id = ?", userID).First(&user).Error; err != nil {
		return nil, err
	}
	return &user, nil
}

func (r *UserRepo) Create(ctx context.Context, user *model.User) error {
	return r.db.WithContext(ctx).Create(user).Error
}

var _ interface {
	FindByUsername(context.Context, string) (*model.User, error)
} = (*UserRepo)(nil)
