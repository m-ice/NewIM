package api

// ErrorCode is a stable process or HTTP failure code.
// ErrorCode 是稳定的进程或 HTTP 失败码。
type ErrorCode string

const (
	CodeInvalidArgument      ErrorCode = "SERVER_INVALID_ARGUMENT"
	CodeInvalidConfig        ErrorCode = "SERVER_INVALID_CONFIG"
	CodeAlreadyStarted       ErrorCode = "SERVER_ALREADY_STARTED"
	CodeBindFailed           ErrorCode = "SERVER_BIND_FAILED"
	CodeRuntimeFailed        ErrorCode = "SERVER_RUNTIME_FAILURE"
	CodeShutdownFailed       ErrorCode = "SERVER_SHUTDOWN_FAILED"
	CodeBuildInfoUnavailable ErrorCode = "SERVER_BUILD_METADATA_UNAVAILABLE"
	CodeInvalidAuthConfig    ErrorCode = "SERVER_INVALID_AUTH_CONFIG"
	CodeInvalidMessageConfig ErrorCode = "SERVER_INVALID_MESSAGE_CONFIG"
)

const (
	HTTPBodyNotAllowed       = "HTTP_BODY_NOT_ALLOWED"
	HTTPBodyTooLarge         = "HTTP_BODY_TOO_LARGE"
	HTTPInvalidQuery         = "HTTP_INVALID_QUERY"
	HTTPRouteNotFound        = "HTTP_ROUTE_NOT_FOUND"
	HTTPMethodNotAllowed     = "HTTP_METHOD_NOT_ALLOWED"
	HTTPUnsupportedMediaType = "HTTP_UNSUPPORTED_MEDIA_TYPE"
	HTTPInternalError        = "HTTP_INTERNAL_ERROR"
	ServerNotReady           = "SERVER_NOT_READY"
)

// Error exposes only a stable code, never a wrapped transport or storage error.
// Error 只暴露稳定错误码，不携带底层传输或存储错误。
type Error struct {
	Code ErrorCode
}

func (e *Error) Error() string { return string(e.Code) }

func fail(code ErrorCode) error { return &Error{Code: code} }
