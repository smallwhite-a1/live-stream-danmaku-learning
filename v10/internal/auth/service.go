package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/charlesAcmen/livestream-danmaku/v10/internal/model"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrInvalidToken       = errors.New("invalid token")
	ErrMissingToken       = errors.New("missing token")
	ErrUserDisabled       = errors.New("user disabled")
)

type UserStore interface {
	FindByUsername(ctx context.Context, username string) (*model.User, error)
}

type Config struct {
	Secret       string
	Issuer       string
	AccessTTL    time.Duration
	CookieName   string
	SecureCookie bool
}

type Claims struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

type Service struct {
	users        UserStore
	secret       []byte
	issuer       string
	accessTTL    time.Duration
	cookieName   string
	secureCookie bool
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	ExpiresAt   time.Time `json:"expires_at"`
	User        UserInfo  `json:"user"`
}

type UserInfo struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
}

func New(users UserStore, config Config) (*Service, error) {
	if users == nil {
		return nil, errors.New("user store is required")
	}
	if len(config.Secret) < 32 {
		return nil, errors.New("jwt secret must contain at least 32 bytes")
	}
	if strings.TrimSpace(config.Issuer) == "" {
		config.Issuer = "v10"
	}
	if config.AccessTTL <= 0 {
		config.AccessTTL = 15 * time.Minute
	}
	if config.CookieName == "" {
		config.CookieName = "v10_access_token"
	}

	return &Service{
		users:        users,
		secret:       []byte(config.Secret),
		issuer:       config.Issuer,
		accessTTL:    config.AccessTTL,
		cookieName:   config.CookieName,
		secureCookie: config.SecureCookie,
	}, nil
}

func (s *Service) Login(ctx context.Context, username, password string) (string, *Claims, error) {
	user, err := s.users.FindByUsername(ctx, strings.TrimSpace(username))
	if err != nil || user == nil {
		return "", nil, ErrInvalidCredentials
	}
	if user.Status != "" && user.Status != model.UserStatusActive {
		return "", nil, ErrUserDisabled
	}
	if bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
		return "", nil, ErrInvalidCredentials
	}

	now := time.Now()
	claims := &Claims{
		UserID:   user.ID,
		Username: user.Username,
		Role:     user.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   user.ID,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.accessTTL)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(s.secret)
	if err != nil {
		return "", nil, err
	}
	return signed, claims, nil
}

func (s *Service) Parse(raw string) (*Claims, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrMissingToken
	}

	claims := &Claims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, ErrInvalidToken
		}
		return s.secret, nil
	})
	if err != nil || !token.Valid || claims.Issuer != s.issuer || claims.UserID == "" {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

func (s *Service) ClaimsFromRequest(r *http.Request) (*Claims, error) {
	if token := bearerToken(r.Header.Get("Authorization")); token != "" {
		return s.Parse(token)
	}
	if cookie, err := r.Cookie(s.cookieName); err == nil && cookie.Value != "" {
		return s.Parse(cookie.Value)
	}
	// Query tokens are kept for CLI clients and local benchmark tools. Browser
	// clients should use the HttpOnly cookie set by LoginHandler.
	if token := strings.TrimSpace(r.URL.Query().Get("token")); token != "" {
		return s.Parse(token)
	}
	return nil, ErrMissingToken
}

func (s *Service) LoginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var input LoginRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4*1024)).Decode(&input); err != nil {
		http.Error(w, "invalid login request", http.StatusBadRequest)
		return
	}

	token, claims, err := s.Login(r.Context(), input.Username, input.Password)
	if err != nil {
		http.Error(w, "invalid username or password", http.StatusUnauthorized)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     s.cookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(s.accessTTL.Seconds()),
		HttpOnly: true,
		Secure:   s.secureCookie,
		SameSite: http.SameSiteLaxMode,
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(LoginResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresAt:   claims.ExpiresAt.Time,
		User: UserInfo{
			ID:       claims.UserID,
			Username: claims.Username,
			Role:     claims.Role,
		},
	})
}

func (s *Service) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     s.cookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   s.secureCookie,
		SameSite: http.SameSiteLaxMode,
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) Require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := s.ClaimsFromRequest(r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		contextWithClaims := context.WithValue(r.Context(), claimsContextKey{}, claims)
		next.ServeHTTP(w, r.WithContext(contextWithClaims))
	})
}

func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey{}).(*Claims)
	return claims, ok
}

type claimsContextKey struct{}

func bearerToken(value string) string {
	parts := strings.Fields(value)
	if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
		return parts[1]
	}
	return ""
}
