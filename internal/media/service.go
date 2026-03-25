package media

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"wire-server/pkg/db/sqlc"
)

type Service struct {
	bucket      string
	region      string
	accessKeyID string
	secretKey   string
	repo        *sqlc.Queries
}

type PresignRequest struct {
	UserID      string
	ObjectKey   string
	ContentType string
	SizeBytes   int64
}

type PresignResponse struct {
	URL           string
	Method        string
	Headers       map[string]string
	ExpiresAt     time.Time
	MediaObjectID string
}

func New(bucket, region, accessKeyID, secretKey string, repo *sqlc.Queries) *Service {
	return &Service{
		bucket:      bucket,
		region:      region,
		accessKeyID: accessKeyID,
		secretKey:   secretKey,
		repo:        repo,
	}
}

func (s *Service) PresignUpload(ctx context.Context, req PresignRequest) (PresignResponse, error) {
	now := time.Now().UTC()
	expires := 15 * time.Minute
	objectID := newID("media")
	key := req.ObjectKey
	if key == "" {
		key = path.Join("uploads", req.UserID, objectID)
	}
	escapedKey := escapeS3Key(key)
	endpoint := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", s.bucket, s.region, escapedKey)
	date := now.Format("20060102T150405Z")
	scopeDate := now.Format("20060102")
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", scopeDate, s.region)
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", s.accessKeyID+"/"+credentialScope)
	query.Set("X-Amz-Date", date)
	query.Set("X-Amz-Expires", fmt.Sprintf("%d", int(expires.Seconds())))
	query.Set("X-Amz-SignedHeaders", "host")
	canonicalRequest := strings.Join([]string{
		"PUT",
		"/" + escapedKey,
		query.Encode(),
		"host:" + s.bucket + ".s3." + s.region + ".amazonaws.com\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")
	signingKey := s.signingKey(scopeDate)
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		date,
		credentialScope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	query.Set("X-Amz-Signature", signature)
	rawURL := endpoint + "?" + query.Encode()
	if s.repo != nil {
		if _, err := s.repo.InsertMediaObject(ctx, sqlc.InsertMediaObjectParams{
			ID:          objectID,
			OwnerID:     req.UserID,
			Bucket:      s.bucket,
			ObjectKey:   key,
			ContentType: req.ContentType,
			SizeBytes:   req.SizeBytes,
			Status:      "pending",
		}); err != nil {
			return PresignResponse{}, fmt.Errorf("media.PresignUpload insert media: %w", err)
		}
	}
	return PresignResponse{
		URL:           rawURL,
		Method:        "PUT",
		Headers:       map[string]string{"Content-Type": req.ContentType},
		ExpiresAt:     now.Add(expires),
		MediaObjectID: objectID,
	}, nil
}

func (s *Service) signingKey(date string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+s.secretKey), date)
	kRegion := hmacSHA256(kDate, s.region)
	kService := hmacSHA256(kRegion, "s3")
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func newID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
}

func escapeS3Key(key string) string {
	parts := strings.Split(key, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
