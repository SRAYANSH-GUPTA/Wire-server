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
	r.With(middleware.AuthMiddleware(s.Config.JWTSecret, s.Config.SupabaseURL)).Get("/ws/connect", s.wsConnect)

	r.Route("/api/v1", func(api chi.Router) {
		api.Use(middleware.AuthMiddleware(s.Config.JWTSecret, s.Config.SupabaseURL))
		api.Post("/messages", s.createMessage)
		api.Get("/messages/{id}", s.getMessage)
		api.Post("/media/upload", s.requestUpload)
		api.Post("/calls/start", s.startCall)
		api.Get("/users/{phone}", s.getUser)
		api.Get("/presence/{phone}", s.getPresence)
		api.Post("/users/lookup", s.lookupUsers)
		api.Post("/groups", s.createGroup)
		api.Get("/groups", s.listGroups)
		api.Get("/groups/{id}", s.getGroup)
		api.Patch("/groups/{id}", s.updateGroup)
		api.Post("/groups/{id}/members", s.addGroupMembers)
		api.Delete("/groups/{id}/members/{phone}", s.removeGroupMember)
		api.Post("/groups/{id}/admins/{phone}", s.promoteGroupAdmin)
		api.Delete("/groups/{id}/admins/{phone}", s.demoteGroupAdmin)
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
		writeError(w, http.StatusBadGateway, "proxy failed")
		return
	}
	req.Header = r.Header.Clone()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "upstream failed")
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
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		writeError(w, http.StatusBadRequest, "upgrade failed")
		return
	}
	c, err := NewConnection(conn, claims)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "connection failed")
		return
	}
	s.Hub.Register(c)
}

func (s *Server) createMessage(w http.ResponseWriter, r *http.Request) {
	claims, _ := middleware.ClaimsFromContext(r.Context())
	var body chatpb.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.Logger.Error("invalid message payload", zap.Error(err))
		writeError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	body.SenderPhone = claims.Phone

	s.Logger.Debug("sending message to chat service", zap.Any("request", &body))
	resp, err := s.GRPCClients.Chat.SendMessage(r.Context(), &body)
	if err != nil {
		s.Logger.Error("chat service grpc failed", zap.Error(err), zap.String("sender", body.SenderPhone), zap.Any("recipients", body.RecipientPhones))
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("grpc failed: %v", err))
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
		s.Logger.Error("chat get history grpc failed", zap.Error(err), zap.String("conversation_id", id))
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("grpc failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) requestUpload(w http.ResponseWriter, r *http.Request) {
	claims, _ := middleware.ClaimsFromContext(r.Context())
	var body mediapb.RequestUploadURLRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	// Inject owner phone from JWT
	body.OwnerPhone = claims.Phone

	resp, err := s.GRPCClients.Media.RequestUploadURL(r.Context(), &body)
	if err != nil {
		s.Logger.Error("media upload grpc failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("grpc failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) startCall(w http.ResponseWriter, r *http.Request) {
	claims, _ := middleware.ClaimsFromContext(r.Context())
	var body callpb.InitiateCallRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	// Inject caller phone from JWT
	body.CallerPhone = claims.Phone

	resp, err := s.GRPCClients.Call.InitiateCall(r.Context(), &body)
	if err != nil {
		s.Logger.Error("call initiate grpc failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("grpc failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getUser(w http.ResponseWriter, r *http.Request) {
	phone := chi.URLParam(r, "phone")
	resp, err := s.GRPCClients.User.GetUser(r.Context(), &userpb.GetUserRequest{UserPhone: phone})
	if err != nil {
		s.Logger.Error("user get grpc failed", zap.Error(err), zap.String("phone", phone))
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("grpc failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getPresence(w http.ResponseWriter, r *http.Request) {
	phone := chi.URLParam(r, "phone")
	resp, err := s.GRPCClients.Presence.GetPresence(r.Context(), &presencepb.GetPresenceRequest{UserPhone: phone})
	if err != nil {
		s.Logger.Error("presence get grpc failed", zap.Error(err), zap.String("phone", phone))
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("grpc failed: %v", err))
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
		writeError(w, http.StatusBadRequest, "invalid payload")
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
		writeError(w, http.StatusInternalServerError, "db failed")
		return
	}
	registered := make([]string, 0, len(users))
	for _, u := range users {
		if u.Valid {
			registered = append(registered, u.String)
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
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"result":  payload,
	})
}

func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": false,
		"error":   message,
	})
}

func copyHeader(dst, src http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

type createGroupRequest struct {
	Name         string   `json:"name"`
	Description  string   `json:"description"`
	AvatarURL    string   `json:"avatar_url"`
	MemberPhones []string `json:"member_phones"`
}

type updateGroupRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	AvatarURL   *string `json:"avatar_url"`
}

type groupMembersRequest struct {
	MemberPhones []string `json:"member_phones"`
}

type joinByInviteRequest struct {
	InviteCode string `json:"invite_code"`
}

type groupMemberResponse struct {
	UserPhone string    `json:"user_phone"`
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
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	var req createGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	memberPhones := dedupeIDs(req.MemberPhones, userPhone)
	groupID := fmt.Sprintf("grp_%d", time.Now().UTC().UnixNano())
	inviteCode, err := randomHex(12)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "invite generation failed")
		return
	}

	tx, err := s.DB.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db begin failed")
		return
	}
	defer tx.Rollback(r.Context())

	_, err = tx.Exec(r.Context(), `
		INSERT INTO groups (id, name, description, avatar_url, created_by_phone, invite_code, created_at, updated_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, NOW(), NOW())
	`, groupID, req.Name, req.Description, req.AvatarURL, userPhone, inviteCode)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "group create failed")
		return
	}
	_, err = tx.Exec(r.Context(), `
		INSERT INTO group_members (group_id, user_phone, role, added_by, created_at)
		VALUES ($1, $2, 'admin', $2, NOW())
		ON CONFLICT (group_id, user_phone) DO UPDATE SET role = 'admin'
	`, groupID, userPhone)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "creator membership failed")
		return
	}
	for _, memberPhone := range memberPhones {
		_, err := tx.Exec(r.Context(), `
			INSERT INTO group_members (group_id, user_phone, role, added_by, created_at)
			VALUES ($1, $2, 'member', $3, NOW())
			ON CONFLICT (group_id, user_phone) DO NOTHING
		`, groupID, memberPhone, userPhone)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "add member failed")
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "db commit failed")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"group_id":    groupID,
		"invite_code": inviteCode,
	})
}

func (s *Server) listGroups(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	rows, err := s.DB.Query(r.Context(), `
		SELECT g.id, g.name, g.description, g.avatar_url, g.created_by_phone, g.invite_code, g.created_at, g.updated_at,
			   (SELECT COUNT(*) FROM group_members gm2 WHERE gm2.group_id = g.id)::int AS member_count
		FROM groups g
		INNER JOIN group_members gm ON gm.group_id = g.id
		WHERE gm.user_phone = $1
		ORDER BY g.updated_at DESC
	`, userPhone)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db failed")
		return
	}
	defer rows.Close()
	out := make([]groupResponse, 0)
	for rows.Next() {
		var g groupResponse
		if err := rows.Scan(&g.ID, &g.Name, &g.Description, &g.AvatarURL, &g.CreatedBy, &g.InviteCode, &g.CreatedAt, &g.UpdatedAt, &g.MemberCount); err != nil {
			writeError(w, http.StatusInternalServerError, "scan failed")
			return
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "db rows failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": out})
}

func (s *Server) getGroup(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	if groupID == "" {
		writeError(w, http.StatusBadRequest, "missing group id")
		return
	}
	if !s.isGroupMember(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	var g groupResponse
	err := s.DB.QueryRow(r.Context(), `
		SELECT id, name, description, avatar_url, created_by_phone, invite_code, created_at, updated_at,
			   (SELECT COUNT(*) FROM group_members WHERE group_id = $1)::int
		FROM groups
		WHERE id = $1
	`, groupID).Scan(&g.ID, &g.Name, &g.Description, &g.AvatarURL, &g.CreatedBy, &g.InviteCode, &g.CreatedAt, &g.UpdatedAt, &g.MemberCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "group not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "db failed")
		return
	}

	memberRows, err := s.DB.Query(r.Context(), `
		SELECT user_phone, role, added_by, created_at
		FROM group_members
		WHERE group_id = $1
		ORDER BY created_at ASC
	`, groupID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "members fetch failed")
		return
	}
	defer memberRows.Close()
	members := make([]groupMemberResponse, 0)
	for memberRows.Next() {
		var m groupMemberResponse
		if err := memberRows.Scan(&m.UserPhone, &m.Role, &m.AddedBy, &m.CreatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, "member scan failed")
			return
		}
		members = append(members, m)
	}
	g.Members = members
	writeJSON(w, http.StatusOK, g)
}

func (s *Server) updateGroup(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	if groupID == "" {
		writeError(w, http.StatusBadRequest, "missing group id")
		return
	}
	if !s.isGroupAdmin(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	var req updateGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid payload")
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
		writeError(w, http.StatusInternalServerError, "update failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) addGroupMembers(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	if !s.isGroupAdmin(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	var req groupMembersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	memberPhones := dedupeIDs(req.MemberPhones, "")
	for _, memberPhone := range memberPhones {
		_, err := s.DB.Exec(r.Context(), `
			INSERT INTO group_members (group_id, user_phone, role, added_by, created_at)
			VALUES ($1, $2, 'member', $3, NOW())
			ON CONFLICT (group_id, user_phone) DO NOTHING
		`, groupID, memberPhone, userPhone)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "add member failed")
			return
		}
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) removeGroupMember(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	targetPhone := chi.URLParam(r, "phone")
	if !s.isGroupAdmin(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	if targetPhone == "" {
		writeError(w, http.StatusBadRequest, "missing phone")
		return
	}
	if s.isGroupAdmin(r.Context(), groupID, targetPhone) && s.adminCount(r.Context(), groupID) <= 1 {
		writeError(w, http.StatusBadRequest, "cannot remove last admin")
		return
	}
	_, err := s.DB.Exec(r.Context(), `DELETE FROM group_members WHERE group_id = $1 AND user_phone = $2`, groupID, targetPhone)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "remove failed")
		return
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) promoteGroupAdmin(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	targetPhone := chi.URLParam(r, "phone")
	if !s.isGroupAdmin(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	_, err := s.DB.Exec(r.Context(), `
		UPDATE group_members
		SET role = 'admin'
		WHERE group_id = $1 AND user_phone = $2
	`, groupID, targetPhone)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "promote failed")
		return
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) demoteGroupAdmin(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	targetPhone := chi.URLParam(r, "phone")
	if !s.isGroupAdmin(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	if !s.isGroupAdmin(r.Context(), groupID, targetPhone) {
		writeError(w, http.StatusBadRequest, "target is not admin")
		return
	}
	if s.adminCount(r.Context(), groupID) <= 1 {
		writeError(w, http.StatusBadRequest, "cannot demote last admin")
		return
	}
	_, err := s.DB.Exec(r.Context(), `
		UPDATE group_members
		SET role = 'member'
		WHERE group_id = $1 AND user_phone = $2
	`, groupID, targetPhone)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "demote failed")
		return
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) leaveGroup(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	if groupID == "" {
		writeError(w, http.StatusBadRequest, "missing group id")
		return
	}
	if !s.isGroupMember(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusBadRequest, "not a group member")
		return
	}
	tx, err := s.DB.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db begin failed")
		return
	}
	defer tx.Rollback(r.Context())

	if s.isGroupAdmin(r.Context(), groupID, userPhone) && s.adminCount(r.Context(), groupID) <= 1 {
		var promotePhone string
		err := tx.QueryRow(r.Context(), `
			SELECT user_phone
			FROM group_members
			WHERE group_id = $1 AND user_phone <> $2
			ORDER BY created_at ASC
			LIMIT 1
		`, groupID, userPhone).Scan(&promotePhone)
		if err == nil {
			_, _ = tx.Exec(r.Context(), `
				UPDATE group_members SET role = 'admin'
				WHERE group_id = $1 AND user_phone = $2
			`, groupID, promotePhone)
		}
	}

	_, err = tx.Exec(r.Context(), `DELETE FROM group_members WHERE group_id = $1 AND user_phone = $2`, groupID, userPhone)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "leave failed")
		return
	}
	var memberCount int
	if err := tx.QueryRow(r.Context(), `SELECT COUNT(*) FROM group_members WHERE group_id = $1`, groupID).Scan(&memberCount); err == nil && memberCount == 0 {
		_, _ = tx.Exec(r.Context(), `DELETE FROM groups WHERE id = $1`, groupID)
	} else {
		_, _ = tx.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "db commit failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) getGroupInvite(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	if !s.isGroupAdmin(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	var code string
	if err := s.DB.QueryRow(r.Context(), `SELECT invite_code FROM groups WHERE id = $1`, groupID).Scan(&code); err != nil {
		writeError(w, http.StatusNotFound, "group not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invite_code": code})
}

func (s *Server) resetGroupInvite(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	groupID := chi.URLParam(r, "id")
	if !s.isGroupAdmin(r.Context(), groupID, userPhone) {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	code, err := randomHex(12)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "invite generation failed")
		return
	}
	_, err = s.DB.Exec(r.Context(), `UPDATE groups SET invite_code = $2, updated_at = NOW() WHERE id = $1`, groupID, code)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "invite reset failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"invite_code": code})
}

func (s *Server) joinByInvite(w http.ResponseWriter, r *http.Request) {
	userPhone, ok := currentUserPhone(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing claims")
		return
	}
	var req joinByInviteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	req.InviteCode = strings.TrimSpace(req.InviteCode)
	if req.InviteCode == "" {
		writeError(w, http.StatusBadRequest, "invite_code is required")
		return
	}
	var groupID string
	if err := s.DB.QueryRow(r.Context(), `SELECT id FROM groups WHERE invite_code = $1`, req.InviteCode).Scan(&groupID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid invite")
		return
	}
	_, err := s.DB.Exec(r.Context(), `
		INSERT INTO group_members (group_id, user_phone, role, added_by, created_at)
		VALUES ($1, $2, 'member', $2, NOW())
		ON CONFLICT (group_id, user_phone) DO NOTHING
	`, groupID, userPhone)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "join failed")
		return
	}
	_, _ = s.DB.Exec(r.Context(), `UPDATE groups SET updated_at = NOW() WHERE id = $1`, groupID)
	writeJSON(w, http.StatusOK, map[string]any{"group_id": groupID})
}

func currentUserPhone(r *http.Request) (string, bool) {
	claims, ok := middleware.ClaimsFromContext(r.Context())
	if !ok || strings.TrimSpace(claims.Phone) == "" {
		return "", false
	}
	return claims.Phone, true
}

func (s *Server) isGroupMember(ctx context.Context, groupID, phone string) bool {
	var exists bool
	err := s.DB.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM group_members
			WHERE group_id = $1 AND user_phone = $2
		)
	`, groupID, phone).Scan(&exists)
	return err == nil && exists
}

func (s *Server) isGroupAdmin(ctx context.Context, groupID, phone string) bool {
	var exists bool
	err := s.DB.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM group_members
			WHERE group_id = $1 AND user_phone = $2 AND role = 'admin'
		)
	`, groupID, phone).Scan(&exists)
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
