package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-jose/go-jose/v3/jwt"
	auth0 "github.com/pucora/go-auth0/v2"
	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	pucorajose "github.com/pucora/pucora-jose/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const authValidatorAlias = "auth/validator"

type methodAuth struct {
	validator   *auth0.JWTValidator
	scfg        *pucorajose.SignatureConfig
	rejecter    pucorajose.Rejecter
	aclCheck    func(string, map[string]interface{}, []string) bool
	scopesMatch func(string, map[string]interface{}, []string) bool
}

func buildMethodAuth(logger logging.Logger, rejecterF pucorajose.RejecterFactory, extra config.ExtraConfig) (*methodAuth, error) {
	ep := &config.EndpointConfig{ExtraConfig: normalizeValidatorExtra(extra)}
	scfg, err := pucorajose.GetSignatureConfig(ep)
	if err == pucorajose.ErrNoValidatorCfg {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if rejecterF == nil {
		rejecterF = new(pucorajose.NopRejecterFactory)
	}
	validator, err := pucorajose.NewValidator(scfg, fromCookie, fromHeader)
	if err != nil {
		return nil, err
	}
	rejecter := rejecterF.New(logger, ep)
	var aclCheck func(string, map[string]interface{}, []string) bool
	if scfg.RolesKeyIsNested && strings.Contains(scfg.RolesKey, ".") && (len(scfg.RolesKey) < 4 || scfg.RolesKey[:4] != "http") {
		aclCheck = pucorajose.CanAccessNested
	} else {
		aclCheck = pucorajose.CanAccess
	}
	var scopesMatcher func(string, map[string]interface{}, []string) bool
	if len(scfg.Scopes) > 0 && scfg.ScopesKey != "" {
		if scfg.ScopesMatcher == "all" {
			scopesMatcher = pucorajose.ScopesAllMatcher
		} else {
			scopesMatcher = pucorajose.ScopesAnyMatcher
		}
	} else {
		scopesMatcher = pucorajose.ScopesDefaultMatcher
	}
	return &methodAuth{
		validator:   validator,
		scfg:        scfg,
		rejecter:    rejecter,
		aclCheck:    aclCheck,
		scopesMatch: scopesMatcher,
	}, nil
}

func (a *methodAuth) validate(ctx context.Context) error {
	if a == nil {
		return nil
	}
	req, err := requestFromMetadata(ctx, a.scfg.AuthHeaderName, a.scfg.CookieKey)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	token, err := a.validator.ValidateRequest(req)
	if err != nil {
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	claims := map[string]interface{}{}
	if err := a.validator.Claims(req, token, &claims); err != nil {
		return status.Error(codes.Unauthenticated, "invalid token claims")
	}
	if a.rejecter.Reject(claims) {
		return status.Error(codes.Unauthenticated, "token rejected")
	}
	if !a.aclCheck(a.scfg.RolesKey, claims, a.scfg.Roles) {
		return status.Error(codes.PermissionDenied, "insufficient roles")
	}
	if !a.scopesMatch(a.scfg.ScopesKey, claims, a.scfg.Scopes) {
		return status.Error(codes.PermissionDenied, "insufficient scopes")
	}
	return nil
}

func normalizeValidatorExtra(extra config.ExtraConfig) config.ExtraConfig {
	if extra == nil {
		return extra
	}
	if _, ok := extra[pucorajose.ValidatorNamespace]; ok {
		return extra
	}
	if raw, ok := extra[authValidatorAlias]; ok {
		out := config.ExtraConfig{}
		for k, v := range extra {
			out[k] = v
		}
		out[pucorajose.ValidatorNamespace] = raw
		return out
	}
	return extra
}

func requestFromMetadata(ctx context.Context, header, cookieKey string) (*http.Request, error) {
	if header == "" {
		header = "Authorization"
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing metadata")
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
	if vals := md.Get(strings.ToLower(header)); len(vals) > 0 {
		req.Header.Set(header, vals[0])
	} else if vals := md.Get(header); len(vals) > 0 {
		req.Header.Set(header, vals[0])
	}
	addCookiesFromMetadata(req, md, cookieKey)
	if req.Header.Get(header) == "" && !hasAuthCookie(req, cookieKey) {
		return nil, fmt.Errorf("missing authorization metadata")
	}
	return req, nil
}

func addCookiesFromMetadata(req *http.Request, md metadata.MD, cookieKey string) {
	if cookieKey == "" {
		cookieKey = "access_token"
	}
	for _, key := range []string{"cookie", "grpcgateway-cookie"} {
		for _, raw := range md.Get(key) {
			for _, part := range strings.Split(raw, ";") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				name, value, ok := strings.Cut(part, "=")
				if !ok {
					continue
				}
				req.AddCookie(&http.Cookie{Name: strings.TrimSpace(name), Value: strings.TrimSpace(value)})
			}
		}
	}
	if vals := md.Get(cookieKey); len(vals) > 0 {
		req.AddCookie(&http.Cookie{Name: cookieKey, Value: vals[0]})
	}
}

func hasAuthCookie(req *http.Request, cookieKey string) bool {
	if cookieKey == "" {
		cookieKey = "access_token"
	}
	if _, err := req.Cookie(cookieKey); err == nil {
		return true
	}
	return len(req.Cookies()) > 0
}

func fromCookie(key string) func(r *http.Request) (*jwt.JSONWebToken, error) {
	if key == "" {
		key = "access_token"
	}
	return func(r *http.Request) (*jwt.JSONWebToken, error) {
		cookie, err := r.Cookie(key)
		if err != nil {
			return nil, auth0.ErrTokenNotFound
		}
		return jwt.ParseSigned(cookie.Value)
	}
}

func fromHeader(header string) func(r *http.Request) (*jwt.JSONWebToken, error) {
	if header == "" {
		header = "Authorization"
	}
	return func(r *http.Request) (*jwt.JSONWebToken, error) {
		raw := r.Header.Get(header)
		if len(raw) > 7 && strings.EqualFold(raw[0:7], "BEARER ") {
			raw = raw[7:]
		}
		if raw == "" {
			return nil, auth0.ErrTokenNotFound
		}
		return jwt.ParseSigned(raw)
	}
}
