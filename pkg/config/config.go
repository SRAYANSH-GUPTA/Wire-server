package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type SharedConfig struct {
	AppEnv         string
	LogLevel       string
	OtelEndpoint   string
	PrometheusPort string
}

type GatewayConfig struct {
	SharedConfig
	Port                string
	JWTSecret           string
	RedisClusterAddrs   string
	ChatServiceAddr     string
	CallServiceAddr     string
	MediaServiceAddr    string
	PresenceServiceAddr string
	UserServiceAddr     string
	SupabaseURL         string
	SupabaseDBURL       string
	EventBusDriver      string
}

type ChatConfig struct {
	SharedConfig
	PrimaryDBURL      string
	ReplicaDBURL      string
	RedisClusterAddrs string
	EventBusDriver    string
}

type CallConfig struct {
	SharedConfig
	TurnSecret       string
	TurnHost         string
	LivekitURL       string
	LivekitAPIKey    string
	LivekitAPISecret string
}

type MediaConfig struct {
	SharedConfig
	S3Bucket            string
	S3Region            string
	CloudFrontDomain    string
	CloudFrontKeyPairID string
	AwsRoleArn          string
}

type PresenceConfig struct {
	SharedConfig
	RedisClusterAddrs string
}

type NotificationConfig struct {
	SharedConfig
	FCMServerKey string
	APNSTeamID   string
}

type UserConfig struct {
	SharedConfig
	ProfileDBURL string
}

func loadShared(v *viper.Viper) SharedConfig {
	v.SetDefault("APP_ENV", "production")
	v.SetDefault("LOG_LEVEL", "info")
	v.SetDefault("OTEL_EXPORTER_JAEGER_ENDPOINT", "http://jaeger:14268/api/traces")
	v.SetDefault("PROMETHEUS_PORT", "9090")
	return SharedConfig{
		AppEnv:         v.GetString("APP_ENV"),
		LogLevel:       v.GetString("LOG_LEVEL"),
		OtelEndpoint:   v.GetString("OTEL_EXPORTER_JAEGER_ENDPOINT"),
		PrometheusPort: v.GetString("PROMETHEUS_PORT"),
	}
}

func assertNonEmpty(name, value string) {
	if strings.TrimSpace(value) == "" {
		panic(fmt.Sprintf("config: %s cannot be empty", name))
	}
}

func loadViper() *viper.Viper {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	return v
}

func LoadGatewayConfig() GatewayConfig {
	v := loadViper()
	cfg := GatewayConfig{
		SharedConfig:        loadShared(v),
		Port:                v.GetString("GATEWAY_PORT"),
		JWTSecret:           v.GetString("JWT_SECRET"),
		RedisClusterAddrs:   v.GetString("REDIS_CLUSTER_ADDRS"),
		ChatServiceAddr:     v.GetString("CHAT_SERVICE_ADDR"),
		CallServiceAddr:     v.GetString("CALL_SERVICE_ADDR"),
		MediaServiceAddr:    v.GetString("MEDIA_SERVICE_ADDR"),
		PresenceServiceAddr: v.GetString("PRESENCE_SERVICE_ADDR"),
		UserServiceAddr:     v.GetString("USER_SERVICE_ADDR"),
		SupabaseURL:         v.GetString("SUPABASE_URL"),
		SupabaseDBURL:       v.GetString("SUPABASE_DB_URL"),
		EventBusDriver:      v.GetString("EVENT_BUS_DRIVER"),
	}
	assertNonEmpty("GATEWAY_PORT", cfg.Port)
	assertNonEmpty("JWT_SECRET", cfg.JWTSecret)
	assertNonEmpty("REDIS_CLUSTER_ADDRS", cfg.RedisClusterAddrs)
	assertNonEmpty("CHAT_SERVICE_ADDR", cfg.ChatServiceAddr)
	assertNonEmpty("CALL_SERVICE_ADDR", cfg.CallServiceAddr)
	assertNonEmpty("MEDIA_SERVICE_ADDR", cfg.MediaServiceAddr)
	assertNonEmpty("PRESENCE_SERVICE_ADDR", cfg.PresenceServiceAddr)
	assertNonEmpty("USER_SERVICE_ADDR", cfg.UserServiceAddr)
	assertNonEmpty("SUPABASE_URL", cfg.SupabaseURL)
	assertNonEmpty("SUPABASE_DB_URL", cfg.SupabaseDBURL)
	return cfg
}

func LoadChatConfig() ChatConfig {
	v := loadViper()
	cfg := ChatConfig{
		SharedConfig:      loadShared(v),
		PrimaryDBURL:      v.GetString("DATABASE_PRIMARY_URL"),
		ReplicaDBURL:      v.GetString("DATABASE_REPLICA_URL"),
		RedisClusterAddrs: v.GetString("REDIS_CLUSTER_ADDRS"),
		EventBusDriver:    v.GetString("EVENT_BUS_DRIVER"),
	}
	assertNonEmpty("DATABASE_PRIMARY_URL", cfg.PrimaryDBURL)
	assertNonEmpty("DATABASE_REPLICA_URL", cfg.ReplicaDBURL)
	assertNonEmpty("REDIS_CLUSTER_ADDRS", cfg.RedisClusterAddrs)
	return cfg
}

func LoadCallConfig() CallConfig {
	v := loadViper()
	cfg := CallConfig{
		SharedConfig:     loadShared(v),
		TurnSecret:       v.GetString("TURN_SECRET"),
		TurnHost:         v.GetString("TURN_HOST"),
		LivekitURL:       v.GetString("LIVEKIT_URL"),
		LivekitAPIKey:    v.GetString("LIVEKIT_API_KEY"),
		LivekitAPISecret: v.GetString("LIVEKIT_API_SECRET"),
	}
	assertNonEmpty("TURN_SECRET", cfg.TurnSecret)
	assertNonEmpty("TURN_HOST", cfg.TurnHost)
	return cfg
}

func LoadMediaConfig() MediaConfig {
	v := loadViper()
	cfg := MediaConfig{
		SharedConfig:        loadShared(v),
		S3Bucket:            v.GetString("S3_BUCKET"),
		S3Region:            v.GetString("S3_REGION"),
		CloudFrontDomain:    v.GetString("CLOUDFRONT_DOMAIN"),
		CloudFrontKeyPairID: v.GetString("CLOUDFRONT_KEY_PAIR_ID"),
		AwsRoleArn:          v.GetString("AWS_ROLE_ARN"),
	}
	assertNonEmpty("S3_BUCKET", cfg.S3Bucket)
	assertNonEmpty("S3_REGION", cfg.S3Region)
	assertNonEmpty("CLOUDFRONT_DOMAIN", cfg.CloudFrontDomain)
	return cfg
}

func LoadPresenceConfig() PresenceConfig {
	v := loadViper()
	cfg := PresenceConfig{
		SharedConfig:      loadShared(v),
		RedisClusterAddrs: v.GetString("REDIS_CLUSTER_ADDRS"),
	}
	assertNonEmpty("REDIS_CLUSTER_ADDRS", cfg.RedisClusterAddrs)
	return cfg
}

func LoadNotificationConfig() NotificationConfig {
	v := loadViper()
	cfg := NotificationConfig{
		SharedConfig: loadShared(v),
		FCMServerKey: v.GetString("FCM_SERVER_KEY"),
		APNSTeamID:   v.GetString("APNS_TEAM_ID"),
	}
	assertNonEmpty("FCM_SERVER_KEY", cfg.FCMServerKey)
	return cfg
}

func LoadUserConfig() UserConfig {
	v := loadViper()
	cfg := UserConfig{
		SharedConfig: loadShared(v),
		ProfileDBURL: v.GetString("PROFILE_DB_URL"),
	}
	assertNonEmpty("PROFILE_DB_URL", cfg.ProfileDBURL)
	return cfg
}
