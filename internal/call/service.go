package call

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"time"
)

type Service struct {
	secret string
}

func New(secret string) *Service {
	return &Service{secret: secret}
}

func (s *Service) TURNCredential(userID string, ttl time.Duration) (username, password string, expiresAt time.Time) {
	expiresAt = time.Now().UTC().Add(ttl)
	username = fmt.Sprintf("%d:%s", expiresAt.Unix(), userID)
	mac := hmac.New(sha1.New, []byte(s.secret))
	_, _ = mac.Write([]byte(username))
	password = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return username, password, expiresAt
}

type SignalType string

const (
	SignalOffer     SignalType = "call.offer"
	SignalAnswer    SignalType = "call.answer"
	SignalCandidate SignalType = "call.candidate"
)

type Signal struct {
	Type     SignalType
	CallID   string
	FromUser string
	ToUser   string
	Payload  []byte
	TraceID  string
}
