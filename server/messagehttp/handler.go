// Package messagehttp exposes the exact loopback text-message send boundary.
// messagehttp 提供精确的 loopback 文本消息发送 HTTP 边界，不拥有持久化事务。
package messagehttp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strings"

	protocol "github.com/m-ice/NewIM/core/protocol/go"
	"github.com/m-ice/NewIM/server/auth/bearerhttp"
	app "github.com/m-ice/NewIM/server/auth/session"
	messageservice "github.com/m-ice/NewIM/server/message"
)

const (
	// Route is the exact message-send path handled by Handler.
	// Route 是 Handler 处理的精确消息发送路径。
	Route = "/api/v1/messages"
)

// Code is a stable HTTP boundary error code with no sensitive data.
// Code 是不含敏感数据的稳定 HTTP 边界错误码。
type Code string

const (
	CodeInvalidQuery         Code = "HTTP_INVALID_QUERY"
	CodeInvalidInput         Code = "SEND_INVALID_INPUT"
	CodeInvalidToken         Code = "AUTH_INVALID_TOKEN"
	CodeUnauthorized         Code = "SEND_UNAUTHORIZED"
	CodeConversationNotFound Code = "SEND_CONVERSATION_NOT_FOUND"
	CodeMethodNotAllowed     Code = "HTTP_METHOD_NOT_ALLOWED"
	CodeRouteNotFound        Code = "HTTP_ROUTE_NOT_FOUND"
	CodeIDConflict           Code = "SEND_ID_CONFLICT"
	CodeSequenceExhausted    Code = "SEND_SEQUENCE_EXHAUSTED"
	CodeBodyTooLarge         Code = "HTTP_BODY_TOO_LARGE"
	CodeUnsupportedMediaType Code = "HTTP_UNSUPPORTED_MEDIA_TYPE"
	CodeInternalError        Code = "HTTP_INTERNAL_ERROR"
	CodeSendUnknown          Code = "SEND_UNKNOWN"
	CodeAuthUnavailable      Code = "AUTH_UNAVAILABLE"
	CodeTemporaryUnavailable Code = "SERVER_TEMPORARY_UNAVAILABLE"
	bearerChallenge               = `Bearer realm="newim-session"`
)

var errInvalidConfiguration = errors.New("messagehttp: invalid configuration")
var errBodyTooLarge = errors.New("messagehttp: body too large")

// Authenticator is implemented by the policy-neutral session service.
// Authenticator 由策略无关的 session service 实现。
type Authenticator interface {
	AuthenticateBearer(context.Context, string) (app.BearerSession, error)
}

// Sender is the narrow trusted send application boundary.
// Sender 是窄化的受信发送应用边界。
type Sender interface {
	Send(context.Context, app.ConnectionIdentity, protocol.Send) (protocol.ServerFrame, error)
}

var _ Authenticator = (*app.Service)(nil)
var _ Sender = (*messageservice.Service)(nil)

// Handler handles exactly POST /api/v1/messages when mounted by a trusted composer.
// Handler 在被受信组合器装配后只处理 POST /api/v1/messages。
type Handler struct {
	authenticator Authenticator
	sender        Sender
	maxBodyBytes  int64
}

// NewHandler validates the trusted dependencies and positive decoded-body limit.
// NewHandler 校验受信依赖与正数解码后请求体上限。
func NewHandler(authenticator Authenticator, sender Sender, maxBodyBytes int64) (*Handler, error) {
	if isNil(authenticator) || isNil(sender) || maxBodyBytes <= 0 {
		return nil, errInvalidConfiguration
	}
	return &Handler{authenticator: authenticator, sender: sender, maxBodyBytes: maxBodyBytes}, nil
}

// ServeHTTP applies the frozen route, transport, authentication, protocol and send order.
// ServeHTTP 按冻结的 route、传输、认证、协议与发送顺序执行。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if w == nil {
		return
	}
	recorder := &responseRecorder{ResponseWriter: w}
	defer func() {
		if recovered := recover(); recovered != nil {
			if !recorder.wroteHeader {
				writeError(recorder, http.StatusInternalServerError, CodeInternalError, false)
				return
			}
			panic(recovered)
		}
	}()
	if h == nil || isNil(h.authenticator) || isNil(h.sender) || h.maxBodyBytes <= 0 {
		writeError(recorder, http.StatusInternalServerError, CodeInternalError, false)
		return
	}
	if r == nil {
		writeError(recorder, http.StatusBadRequest, CodeInvalidInput, false)
		return
	}
	if r.URL == nil || r.URL.EscapedPath() != Route {
		writeError(recorder, http.StatusNotFound, CodeRouteNotFound, false)
		return
	}
	if r.Method != http.MethodPost {
		recorder.Header().Set("Allow", http.MethodPost)
		writeError(recorder, http.StatusMethodNotAllowed, CodeMethodNotAllowed, false)
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.Fragment != "" || r.URL.RawFragment != "" {
		writeError(recorder, http.StatusBadRequest, CodeInvalidQuery, false)
		return
	}
	if r.ContentLength > h.maxBodyBytes {
		writeError(recorder, http.StatusRequestEntityTooLarge, CodeBodyTooLarge, false)
		return
	}
	if !validContentType(r.Header) || !validContentEncoding(r.Header) {
		writeError(recorder, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, false)
		return
	}
	body, err := readBoundedBody(recorder, r, h.maxBodyBytes)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			writeError(recorder, http.StatusRequestEntityTooLarge, CodeBodyTooLarge, false)
			return
		}
		writeError(recorder, http.StatusBadRequest, CodeInvalidInput, false)
		return
	}

	rawToken, ok := bearerhttp.ParseBearerHeader(r.Header.Values("Authorization"))
	if !ok {
		writeUnauthorized(recorder)
		return
	}
	ctx := r.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	session, err := h.authenticator.AuthenticateBearer(ctx, rawToken)
	if err != nil {
		status, _ := bearerhttp.MapAuthenticationError(err)
		if status == http.StatusUnauthorized {
			writeUnauthorized(recorder)
			return
		}
		writeError(recorder, http.StatusServiceUnavailable, CodeAuthUnavailable, false)
		return
	}
	if session.ExpiresAt().IsZero() {
		writeError(recorder, http.StatusServiceUnavailable, CodeAuthUnavailable, false)
		return
	}
	identity, err := app.NewConnectionIdentity(session.UserID(), session.DeviceID(), session.SessionID(), session.TokenID(), session.TokenID())
	if err != nil {
		writeError(recorder, http.StatusServiceUnavailable, CodeAuthUnavailable, false)
		return
	}

	request, err := protocol.DecodeSend(body)
	if err != nil {
		status, code := mapProtocolError(err)
		writeError(recorder, status, code, false)
		return
	}
	if request.ProtocolVersion != 1 || request.Version != 1 || request.Type != "text" {
		writeError(recorder, http.StatusBadRequest, CodeInvalidInput, false)
		return
	}
	frame, err := h.sender.Send(ctx, identity, request)
	if err != nil {
		status, code, retry := mapSendError(err)
		writeError(recorder, status, code, retry)
		return
	}
	if frame.Ack == nil || frame.Error != nil || frame.Message != nil {
		writeError(recorder, http.StatusInternalServerError, CodeSendUnknown, false)
		return
	}
	if err := protocol.Correlate(request, identity.UserID(), frame); err != nil {
		writeError(recorder, http.StatusInternalServerError, CodeSendUnknown, false)
		return
	}
	writeACK(recorder, frame)
}

func validContentType(header http.Header) bool {
	values := header.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	mediaType, params, err := mime.ParseMediaType(values[0])
	if err != nil || mediaType != "application/json" {
		return false
	}
	for name, value := range params {
		if !strings.EqualFold(name, "charset") || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	return true
}

func validContentEncoding(header http.Header) bool {
	values := header.Values("Content-Encoding")
	if len(values) == 0 {
		return true
	}
	if len(values) != 1 {
		return false
	}
	value := strings.TrimSpace(values[0])
	return value == "" || strings.EqualFold(value, "identity")
}

func readBoundedBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	reader := http.MaxBytesReader(w, body, limit)
	defer reader.Close()
	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, errBodyTooLarge
		}
		return nil, err
	}
	return bodyBytes, nil
}

func mapProtocolError(err error) (int, Code) {
	if errors.Is(err, protocol.TooLarge) {
		return http.StatusRequestEntityTooLarge, CodeBodyTooLarge
	}
	return http.StatusBadRequest, CodeInvalidInput
}

func mapSendError(err error) (int, Code, bool) {
	if messageservice.RetryDisposition(err) == protocol.RetrySameIntent {
		return http.StatusServiceUnavailable, CodeTemporaryUnavailable, true
	}
	switch messageservice.ErrorCode(err) {
	case messageservice.SendInvalidInput:
		return http.StatusBadRequest, CodeInvalidInput, false
	case messageservice.SendUnauthorized:
		return http.StatusForbidden, CodeUnauthorized, false
	case messageservice.SendConversationMissing:
		return http.StatusNotFound, CodeConversationNotFound, false
	case messageservice.SendIDConflict:
		return http.StatusConflict, CodeIDConflict, false
	case messageservice.SendSequenceExhausted:
		return http.StatusConflict, CodeSequenceExhausted, false
	default:
		return http.StatusInternalServerError, CodeSendUnknown, false
	}
}

func writeACK(w http.ResponseWriter, frame protocol.ServerFrame) {
	body, err := protocol.EncodeServerFrame(frame)
	if err != nil {
		writeError(w, http.StatusInternalServerError, CodeSendUnknown, false)
		return
	}
	setCommonHeaders(w)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", bearerChallenge)
	writeError(w, http.StatusUnauthorized, CodeInvalidToken, false)
}

func writeError(w http.ResponseWriter, status int, code Code, retry bool) {
	setCommonHeaders(w)
	if retry {
		w.Header().Set("Retry-After", "1")
	}
	body, err := json.Marshal(errorResponse{Error: errorBody{Code: code}})
	if err != nil {
		body = []byte(`{"error":{"code":"HTTP_INTERNAL_ERROR"}}`)
	}
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code Code `json:"code"`
}

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

type responseRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(data)
}
