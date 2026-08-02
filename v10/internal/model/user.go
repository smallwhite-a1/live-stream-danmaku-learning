package model

import "time"

const (
	UserStatusActive   = "active"
	UserStatusDisabled = "disabled"
)

// User stores the identity used by HTTP and WebSocket authentication.
// PasswordHash is a bcrypt hash and must never be returned to clients.
type User struct {
	ID           string    `gorm:"type:varchar(64);primaryKey" json:"id"`
	Username     string    `gorm:"type:varchar(64);not null;uniqueIndex" json:"username"`
	PasswordHash string    `gorm:"type:varchar(255);not null" json:"-"`
	Role         string    `gorm:"type:varchar(32);not null;default:viewer" json:"role"`
	Status       string    `gorm:"type:varchar(16);not null;default:active;index" json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (User) TableName() string {
	return "v10_users"
}
