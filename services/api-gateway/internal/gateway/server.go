package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gobwas/ws"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"wire-server/pkg/config"
	"wire-server/pkg/db/sqlc"
	"wire-server/pkg/eventbus"
	"wire-server/pkg/middleware"
	"wire-server/pkg/proto/callpb"
	"wire-server/pkg/proto/chatpb"
	"wire-server/pkg/proto/mediapb"
	"wire-server/pkg/proto/presencepb"
	"wire-server/pkg/proto/userpb"
	"wire-server/pkg/redis"
	"wire-server/services/api-gateway/internal/clients"
)

type Server struct {
	Config      config.GatewayConfig
	Logger      *zap.Logger
	Hub         *Hub
	EventBus    eventbus.EventBus
	Redis       *redis.Client
	GRPCClients *clients.Clients
	DB          *pgxpool.Pool
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.TracingMiddleware("api-gateway"))
	r.Use(middleware.MetricsMiddleware())
	r.Use(middleware.RateLimitMiddleware(s.Redis.Cmdable(), 200, time.Minute))

	r.Get("/health", s.health)
	r.Get("/metrics", promhttp.Handler().ServeHTTP)
	r.Post("/auth/refresh", s.refreshToken)
	r.With(middleware.AuthMiddleware(s.Config.JWTSecret, s.Config.SupabaseURL)).Post("/ws/connect", s.wsConnect)

	r.Route("/api/v1", func(api chi.Router) {
		api.Use(middleware.AuthMiddleware(s.Config.JWTSecret, s.Config.SupabaseURL))
		api.Post("/messages", s.createMessage)
		api.Get("/messages/{id}", s.getMessage)
		api.Post("/media/upload", s.requestUpload)
		api.Post("/calls/start", s.startCall)
		api.Get("/users/{id}", s.getUser)
		api.Get("/presence/{id}", s.getPresence)
		api.Post("/users/lookup", s.lookupUsers)
		api.Post("/groups", s.createGroup)
		api.Get("/groups", s.listGroups)
		api.Get("/groups/{id}", s.getGroup)
		api.Patch("/groups/{id}", s.updateGroup)
		api.Post("/groups/{id}/members", s.addGroupMembers)
		api.Delete("/groups/{id}/members/{user_id}", s.removeGroupMember)
		api.Post("/groups/{id}/admins/{user_id}", s.promoteGroupAdmin)
		api.Delete("/groups/{id}/admins/{user_id}", s.demoteGroupAdmin)
		api.Post("/groups/{id}/leave", s.leaveGroup)
		api.Get("/groups/{id}/invite", s.getGroupInvite)
		api.Post("/groups/{id}/invite/reset", s.resetGroupInvite)
		api.Post("/groups/join-by-invite", s.joinByInvite)
	})

	return r
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "api-gateway"})
}

func (s *Server) refreshToken(w http.ResponseWriter, r *http.Request) {
	target := fmt.Sprintf("%s/auth/v1/token?grant_type=refresh_token", strings.TrimRight(s.Config.SupabaseURL, "/"))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, target, r.Body)
	if err != nil {
		http.Error(w, "proxy failed", http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "upstream failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) wsConnect(w http.ResponseWriter, r *http.Request) {
	claims, ok := middleware.ClaimsFromContext(r.Context())
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		http.Error(w, "upgrade failed", http.StatusBadRequest)
		return
	}
	c, err := NewConnection(conn, claims)
	if err != nil {
		http.Error(w, "connection failed", http.StatusInternalServerError)
		return
	}
	s.Hub.Register(c)
}

func (s *Server) createMessage(w http.ResponseWriter, r *http.Request) {
	var body chatpb.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	resp, err := s.GRPCClients.Chat.SendMessage(r.Context(), &body)
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getMessage(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	resp, err := s.GRPCClients.Chat.GetHistory(r.Context(), &chatpb.GetHistoryRequest{
		ConversationId: id,
		Limit:          1,
	})
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) requestUpload(w http.ResponseWriter, r *http.Request) {
	var body mediapb.RequestUploadURLRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	resp, err := s.GRPCClients.Media.RequestUploadURL(r.Context(), &body)
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) startCall(w http.ResponseWriter, r *http.Request) {
	var body callpb.InitiateCallRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	resp, err := s.GRPCClients.Call.InitiateCall(r.Context(), &body)
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getUser(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	resp, err := s.GRPCClients.User.GetUser(r.Context(), &userpb.GetUserRequest{UserId: id})
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getPresence(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	resp, err := s.GRPCClients.Presence.GetPresence(r.Context(), &presencepb.GetPresenceRequest{UserId: id})
	if err != nil {
		http.Error(w, "grpc failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type LookupUsersRequest struct {
	Phones []string `json:"phones"`
}

type LookupUsersResponse struct {
	Registered []string `json:"registered"`
	NotFound   []string `json:"not_found"`
}

func (s *Server) lookupUsers(w http.ResponseWriter, r *http.Request) {
	var req LookupUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if len(req.Phones) == 0 {
		writeJSON(w, http.StatusOK, LookupUsersResponse{})
		return
	}
	q := sqlc.New(s.DB)
	users, err := q.FindUsersByPhones(r.Context(), req.Phones)
	if err != nil {
		s.Logger.Error("lookup users", zap.Error(err))
		http.Error(w, "db failed", http.StatusInternalServerError)
		return
	}
	registered := make([]string, 0, len(users))
	for _, u := range users {
		if u.Phone != nil {
			registered = append(registered, *u.Phone)
		}
	}
	phoneSet := make(map[string]bool)
	for _, p := range req.Phones {
		phoneSet[p] = true
	}
	for _, p := range registered {
		delete(phoneSet, p)
	}
	notFound := make([]string, 0, len(phoneSet))
	for p := range phoneSet {
		notFound = append(notFound, p)
	}
	writeJSON(w, http.StatusOK, LookupUsersResponse{
		Registered: registered,
		NotFound:   notFound,
	})
}

func (s *Server) UserExists(ctx context.Context, phone string) (bool, error) {
	q := sqlc.New(s.DB)
	users, err := q.FindUsersByPhones(ctx, []string{phone})
	if err != nil {
		return false, err
	}
	return len(users) > 0, nil
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func copyHeader(dst, src http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

type createGroupRequest struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	AvatarURL   string   `json:"avatar_url"`
	MemberIDs   []string `json:"member_ids"`
}

type updateGroupRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	AvatarURL   *string `json:"avatar_url"`
}

type groupMembersRequest struct {
	MemberIDs []string `json:"member_ids"`
}

type joinByInviteRequest struct {
	InviteCode string `json:"invite_code"`
}

type groupMemberResponse struct {
	UserID    string    `json:"user_id"`
	Role      string    `json:"role"`
	AddedBy   *string   `json:"added_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type groupResponse struct {
	ID          string                `json:"id"`
	Name        string                `json:"name"`
	Description *string               `json:"description,omitempty"`
	AvatarURL   *string               `json:"avatar_url,omitempty"`
	CreatedBy   string                `json:"created_by"`
	InviteCode  string                `json:"invite_code"`
	CreatedAt   time.Time             `json:"created_at"`
	UpdatedAt   time.Time             `json:"updated_at"`
	MemberCount int                   `json:"member_count"`
	Members     []groupMemberResponse `json:"members,omitempty"`
}

func (s *Server) createGroup(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	var req createGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	memberIDs := dedupeIDs(req.MemberIDs, userID)
	groupID := fmt.Sprintf("grp_%d", time.Now().UTC().UnixNano())
	inviteCode, err := randomHex(12)
	if err != nil {
		http.Error(w, "invite generation failed", http.StatusInternalServerError)
		return
	}

	tx, err := s.DB.Begin(r.Context())
	if err != nil {
		http.Error(w, "db begin failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	_, err = tx.Exec(r.Context(), `
		INSERT INTO groups (id, name, description, avatar_url, created_by, invite_code, created_at, updated_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, NOW(), NOW())
	`, groupID, req.Name, req.Description, req.AvatarURL, userID, inviteCode)
	if err != nil {
		http.Error(w, "group create failed", http.StatusInternalServerError)
		return
	}
	_, err = tx.Exec(r.Context(), `
		INSERT INTO group_members (group_id, user_id, role, added_by, created_at)
		VALUES ($1, $2, 'admin', $2, NOW())
		ON CONFLICT (group_id, user_id) DO UPDATE SET role = 'admin'
	`, groupID, userID)
	if err != nil {
		http.Error(w, "creator membership failed", http.StatusInternalServerError)
		return
	}
	for _, memberID := range memberIDs {
		_, err := tx.Exec(r.Context(), `
			INSERT INTO group_members (group_id, user_id, role, added_by, created_at)
			VALUES ($1, $2, 'member', $3, NOW())
			ON CONFLICT (group_id, user_id) DO NOTHING
		`, groupID, memberID, userID)
		if err != nil {
			http.Error(w, "add member failed", http.StatusInternalServerError)
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		http.Error(w, "db commit failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"group_id":    groupID,
		"invite_code": inviteCode,
	})
}

func (s *Server) listGroups(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	rows, err := s.DB.Query(r.Context(), `
		SELECT g.id, g.name, g.description, g.avatar_url, g.created_by, g.invite_code, g.created_at, g.updated_at,
			   (SELECT COUNT(*) FROM group_members gm2 WHERE gm2.group_id = g.id)::int AS member_count
		FROM groups g
		INNER JOIN group_members gm ON gm.group_id = g.id
		WHERE gm.user_id = $1
		ORDER BY g.updated_at DESC
	`, userID)
	if err != nil {
		http.Error(w, "db failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := make([]groupResponse, 0)
	for rows.Next() {
		var g groupResponse
		if err := rows.Scan(&g.ID, &g.Name, &g.Description, &g.AvatarURL, &g.CreatedBy, &g.InviteCode, &g.CreatedAt, &g.UpdatedAt, &g.MemberCount); err != nil {
			http.Error(w, "scan failed", http.StatusInternalServerError)
			return
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "db rows failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": out})
}

func (s *Server) getGroup(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	if groupID == "" {
		http.Error(w, "missing group id", http.StatusBadRequest)
		return
	}
	if !s.isGroupMember(r.Context(), groupID, userID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var g groupResponse
	err := s.DB.QueryRow(r.Context(), `
		SELECT id, name, description, avatar_url, created_by, invite_code, created_at, updated_at,
			   (SELECT COUNT(*) FROM group_members WHERE group_id = $1)::int
		FROM groups
		WHERE id = $1
	`, groupID).Scan(&g.ID, &g.Name, &g.Description, &g.AvatarURL, &g.CreatedBy, &g.InviteCode, &g.CreatedAt, &g.UpdatedAt, &g.MemberCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "group not found", http.StatusNotFound)
			return
		}
		http.Error(w, "db failed", http.StatusInternalServerError)
		return
	}

	memberRows, err := s.DB.Query(r.Context(), `
		SELECT user_id, role, added_by, created_at
		FROM group_members
		WHERE group_id = $1
		ORDER BY created_at ASC
	`, groupID)
	if err != nil {
		http.Error(w, "members fetch failed", http.StatusInternalServerError)
		return
	}
	defer memberRows.Close()
	members := make([]groupMemberResponse, 0)
	for memberRows.Next() {
		var m groupMemberResponse
		if err := memberRows.Scan(&m.UserID, &m.Role, &m.AddedBy, &m.CreatedAt); err != nil {
			http.Error(w, "member scan failed", http.StatusInternalServerError)
			return
		}
		members = append(members, m)
	}
	g.Members = members
	writeJSON(w, http.StatusOK, g)
}

func (s *Server) updateGroup(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	if groupID == "" {
		http.Error(w, "missing group id", http.StatusBadRequest)
		return
	}
	if !s.isGroupAdmin(r.Context(), groupID, userID) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	var req updateGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	_, err := s.DB.Exec(r.Context(), `
		UPDATE groups
		SET name = COALESCE(NULLIF($2, ''), name),
			description = COALESCE($3, description),
			avatar_url = COALESCE($4, avatar_url),
			updated_at = NOW()
		WHERE id = $1
	`, groupID, req.Name, req.Description, req.AvatarURL)
	if err != nil {
		http.Error(w, "update failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) addGroupMembers(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	if !s.isGroupAdmin(r.Context(), groupID, userID) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	var req groupMembersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	memberIDs := dedupeIDs(req.MemberIDs, "")
	for _, memberID := range memberIDs {
		_, err := s.DB.Exec(r.Context(), `
			INSERT INTO group_members (group_id, user_id, role, added_by, created_at)
			VALUES ($1, $2, 'member', $3, NOW())
			ON CONFLICT (group_id, user_id) DO NOTHING
		`, groupID, memberID, userID)
		if err != nil {
			http.Error(w, "add member failed", http.StatusInternalServerError)
			return
		}
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) removeGroupMember(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	targetID := chi.URLParam(r, "user_id")
	if !s.isGroupAdmin(r.Context(), groupID, userID) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	if targetID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	if s.isGroupAdmin(r.Context(), groupID, targetID) && s.adminCount(r.Context(), groupID) <= 1 {
		http.Error(w, "cannot remove last admin", http.StatusBadRequest)
		return
	}
	_, err := s.DB.Exec(r.Context(), `DELETE FROM group_members WHERE group_id = $1 AND user_id = $2`, groupID, targetID)
	if err != nil {
		http.Error(w, "remove failed", http.StatusInternalServerError)
		return
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) promoteGroupAdmin(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	targetID := chi.URLParam(r, "user_id")
	if !s.isGroupAdmin(r.Context(), groupID, userID) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	_, err := s.DB.Exec(r.Context(), `
		UPDATE group_members
		SET role = 'admin'
		WHERE group_id = $1 AND user_id = $2
	`, groupID, targetID)
	if err != nil {
		http.Error(w, "promote failed", http.StatusInternalServerError)
		return
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) demoteGroupAdmin(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	targetID := chi.URLParam(r, "user_id")
	if !s.isGroupAdmin(r.Context(), groupID, userID) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	if !s.isGroupAdmin(r.Context(), groupID, targetID) {
		http.Error(w, "target is not admin", http.StatusBadRequest)
		return
	}
	if s.adminCount(r.Context(), groupID) <= 1 {
		http.Error(w, "cannot demote last admin", http.StatusBadRequest)
		return
	}
	_, err := s.DB.Exec(r.Context(), `
		UPDATE group_members
		SET role = 'member'
		WHERE group_id = $1 AND user_id = $2
	`, groupID, targetID)
	if err != nil {
		http.Error(w, "demote failed", http.StatusInternalServerError)
		return
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) leaveGroup(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	if groupID == "" {
		http.Error(w, "missing group id", http.StatusBadRequest)
		return
	}
	if !s.isGroupMember(r.Context(), groupID, userID) {
		http.Error(w, "not a group member", http.StatusBadRequest)
		return
	}
	tx, err := s.DB.Begin(r.Context())
	if err != nil {
		http.Error(w, "db begin failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	if s.isGroupAdmin(r.Context(), groupID, userID) && s.adminCount(r.Context(), groupID) <= 1 {
		var promoteID string
		err := tx.QueryRow(r.Context(), `
			SELECT user_id
			FROM group_members
			WHERE group_id = $1 AND user_id <> $2
			ORDER BY created_at ASC
			LIMIT 1
		`, groupID, userID).Scan(&promoteID)
		if err == nil {
			_, _ = tx.Exec(r.Context(), `
				UPDATE group_members SET role = 'admin'
				WHERE group_id = $1 AND user_id = $2
			`, groupID, promoteID)
		}
	}

	_, err = tx.Exec(r.Context(), `DELETE FROM group_members WHERE group_id = $1 AND user_id = $2`, groupID, userID)
	if err != nil {
		http.Error(w, "leave failed", http.StatusInternalServerError)
		return
	}
	var memberCount int
	if err := tx.QueryRow(r.Context(), `SELECT COUNT(*) FROM group_members WHERE group_id = $1`, groupID).Scan(&memberCount); err == nil && memberCount == 0 {
		_, _ = tx.Exec(r.Context(), `DELETE FROM groups WHERE id = $1`, groupID)
	} else {
		_, _ = tx.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	}
	if err := tx.Commit(r.Context()); err != nil {
		http.Error(w, "db commit failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) getGroupInvite(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	if !s.isGroupAdmin(r.Context(), groupID, userID) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	var code string
	if err := s.DB.QueryRow(r.Context(), `SELECT invite_code FROM groups WHERE id = $1`, groupID).Scan(&code); err != nil {
		http.Error(w, "group not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invite_code": code})
}

func (s *Server) resetGroupInvite(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	groupID := chi.URLParam(r, "id")
	if !s.isGroupAdmin(r.Context(), groupID, userID) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	code, err := randomHex(12)
	if err != nil {
		http.Error(w, "invite generation failed", http.StatusInternalServerError)
		return
	}
	_, err = s.DB.Exec(r.Context(), `UPDATE groups SET invite_code = $2, updated_at = NOW() WHERE id = $1`, groupID, code)
	if err != nil {
		http.Error(w, "invite reset failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invite_code": code})
}

func (s *Server) joinByInvite(w http.ResponseWriter, r *http.Request) {
	userID, ok := currentUserID(r)
	if !ok {
		http.Error(w, "missing claims", http.StatusUnauthorized)
		return
	}
	var req joinByInviteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	req.InviteCode = strings.TrimSpace(req.InviteCode)
	if req.InviteCode == "" {
		http.Error(w, "invite_code is required", http.StatusBadRequest)
		return
	}
	var groupID string
	if err := s.DB.QueryRow(r.Context(), `SELECT id FROM groups WHERE invite_code = $1`, req.InviteCode).Scan(&groupID); err != nil {
		http.Error(w, "invalid invite", http.StatusBadRequest)
		return
	}
	_, err := s.DB.Exec(r.Context(), `
		INSERT INTO group_members (group_id, user_id, role, added_by, created_at)
		VALUES ($1, $2, 'member', $2, NOW())
		ON CONFLICT (group_id, user_id) DO NOTHING
	`, groupID, userID)
	if err != nil {
		http.Error(w, "join failed", http.StatusInternalServerError)
		return
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"group_id": groupID})
}

func currentUserID(r *http.Request) (string, bool) {
	claims, ok := middleware.ClaimsFromContext(r.Context())
	if !ok || strings.TrimSpace(claims.UserID) == "" {
		return "", false
	}
	return claims.UserID, true
}

func (s *Server) isGroupMember(ctx context.Context, groupID, userID string) bool {
	var exists bool
	err := s.DB.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM group_members
			WHERE group_id = $1 AND user_id = $2
		)
	`, groupID, userID).Scan(&exists)
	return err == nil && exists
}

func (s *Server) isGroupAdmin(ctx context.Context, groupID, userID string) bool {
	var exists bool
	err := s.DB.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM group_members
			WHERE group_id = $1 AND user_id = $2 AND role = 'admin'
		)
	`, groupID, userID).Scan(&exists)
	return err == nil && exists
}

func (s *Server) adminCount(ctx context.Context, groupID string) int {
	var n int
	if err := s.DB.QueryRow(ctx, `SELECT COUNT(*) FROM group_members WHERE group_id = $1 AND role = 'admin'`, groupID).Scan(&n); err != nil {
		return 0
	}
	return n
}

func dedupeIDs(ids []string, skip string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || (skip != "" && id == skip) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
