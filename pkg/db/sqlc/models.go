package sqlc

import "time"

type Message struct {
	ID             string
	ConversationID string
	SenderID       string
	Body           string
	MediaURL       *string
	CreatedAt      time.Time
	RecipientID    *string
	GroupID        *string
	BodyEncrypted  string
	BodyType       string
	ServerTs       time.Time
}

type MessageReceipt struct {
	MessageID string
	UserID    string
	Status    string
	UpdatedAt time.Time
}

type ConversationMember struct {
	ConversationID string
	UserID         string
	CreatedAt      time.Time
}

type MediaObject struct {
	ID          string
	OwnerID     string
	Bucket      string
	ObjectKey   string
	ContentType string
	SizeBytes   int64
	Status      string
	CreatedAt   time.Time
}

type PresenceRecord struct {
	UserID    string
	Status    string
	LastSeen  *time.Time
	UpdatedAt time.Time
}

type User struct {
	ID        string
	Email     string
	Phone     *string
	Role      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type MessageStatus struct {
	MessageID string
	UserID    string
	Status    string
	UpdatedAt time.Time
}
