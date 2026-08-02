package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
	"golang.org/x/crypto/bcrypt"
)

type fakeUserStore struct {
	user *model.User
}

func (s fakeUserStore) FindByUsername(context.Context, string) (*model.User, error) {
	if s.user == nil {
		return nil, errors.New("user not found")
	}
	return s.user, nil
}

func testService(t *testing.T, accessTTL time.Duration) *Service {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("password-1"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	service, err := New(fakeUserStore{user: &model.User{
		ID:           "user-1",
		Username:     "alice",
		PasswordHash: string(hash),
		Role:         "viewer",
		Status:       model.UserStatusActive,
	}}, Config{
		Secret:       strings.Repeat("s", 32),
		Issuer:       "v10-test",
		AccessTTL:    accessTTL,
		CookieName:   "v10_access_token",
		SecureCookie: false,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

func TestLoginIssuesAndParsesHS256Token(t *testing.T) {
	service := testService(t, time.Minute)
	token, claims, err := service.Login(context.Background(), "alice", "password-1")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if token == "" || claims.UserID != "user-1" || claims.Username != "alice" {
		t.Fatalf("unexpected login result token=%q claims=%+v", token, claims)
	}

	parsed, err := service.Parse(token)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed.UserID != "user-1" || parsed.Role != "viewer" || parsed.Issuer != "v10-test" {
		t.Fatalf("unexpected parsed claims: %+v", parsed)
	}
}

func TestLoginRejectsWrongCredentials(t *testing.T) {
	service := testService(t, time.Minute)
	if _, _, err := service.Login(context.Background(), "alice", "wrong-password"); err == nil {
		t.Fatal("Login() error = nil, want invalid credentials")
	}
}

func TestParseRejectsExpiredToken(t *testing.T) {
	service := testService(t, time.Millisecond)
	token, _, err := service.Login(context.Background(), "alice", "password-1")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := service.Parse(token); err == nil {
		t.Fatal("Parse() error = nil, want expired token error")
	}
}

func TestLoginHandlerSetsHttpOnlyCookie(t *testing.T) {
	service := testService(t, time.Minute)
	request := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(`{"username":"alice","password":"password-1"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	service.LoginHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].Name != "v10_access_token" {
		t.Fatalf("unexpected cookies: %+v", cookies)
	}
}

func TestClaimsFromRequestReadsBearerToken(t *testing.T) {
	service := testService(t, time.Minute)
	token, _, err := service.Login(context.Background(), "alice", "password-1")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	claims, err := service.ClaimsFromRequest(request)
	if err != nil {
		t.Fatalf("ClaimsFromRequest() error = %v", err)
	}
	if claims.UserID != "user-1" {
		t.Fatalf("user id = %q, want user-1", claims.UserID)
	}
}
