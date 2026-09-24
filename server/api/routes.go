package api

import (
	"net/http"
	"strings"
)

// APIRoute is a bounded exact route supplied by a trusted process composer.
// AllowedMethods is omitted or nil for the backward-compatible GET default.
// APIRoute 是由受信进程装配的有界精确路由；AllowedMethods 省略或为 nil 时兼容为仅 GET。
type APIRoute struct {
	Name           string
	Path           string
	AllowedMethods []string
	Handler        http.Handler
}

func validateAPIRoutes(routes []APIRoute) (map[string]APIRoute, error) {
	if len(routes) > 8 {
		return nil, fail(CodeInvalidConfig)
	}
	result := make(map[string]APIRoute, len(routes))
	for _, route := range routes {
		if !validRouteName(route.Name) || !validRoutePath(route.Path) || route.Handler == nil {
			return nil, fail(CodeInvalidConfig)
		}
		methods, ok := normalizeAllowedMethods(route.AllowedMethods)
		if !ok {
			return nil, fail(CodeInvalidConfig)
		}
		route.AllowedMethods = methods
		if _, exists := result[route.Path]; exists {
			return nil, fail(CodeInvalidConfig)
		}
		result[route.Path] = route
	}
	return result, nil
}

func normalizeAllowedMethods(methods []string) ([]string, bool) {
	if methods == nil {
		return []string{http.MethodGet}, true
	}
	if len(methods) == 0 {
		return nil, false
	}
	order := []string{http.MethodGet, http.MethodDelete}
	rank := map[string]int{http.MethodGet: 0, http.MethodDelete: 1}
	normalized := make([]string, 0, len(methods))
	lastRank := -1
	for _, method := range methods {
		current, ok := rank[method]
		if !ok || current <= lastRank {
			return nil, false
		}
		normalized = append(normalized, order[current])
		lastRank = current
	}
	return normalized, true
}

func allowsMethod(methods []string, method string) bool {
	for _, allowed := range methods {
		if allowed == method {
			return true
		}
	}
	return false
}

func validRouteName(value string) bool {
	if len(value) == 0 || len(value) > 32 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for i := 1; i < len(value); i++ {
		c := value[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}

func validRoutePath(value string) bool {
	if !strings.HasPrefix(value, "/api/v1/") || len(value) > 128 || strings.HasSuffix(value, "/") || strings.ContainsAny(value, "?#\\") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == ".." {
			return false
		}
	}
	return true
}
