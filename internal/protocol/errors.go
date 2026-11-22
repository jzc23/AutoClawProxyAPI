package protocol

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ErrorKind 定义上游错误的分级类别，供调度器决定冷却、禁用或切号重试策略。
type ErrorKind string

const (
	ErrorKindSessionDead ErrorKind = "session_dead" // 账号会话失效（410000/400000/登录过期）→ 暂停调度
	ErrorKindHardCredit  ErrorKind = "hard_credit"  // 积分或余额不足（HTTP 402/配额文案）→ 长冷却并切号
	ErrorKindSoftRate    ErrorKind = "soft_rate"    // 触发限流（HTTP 429）→ 短冷却并切号
	ErrorKindRiskControl ErrorKind = "risk_control" // HTTP 403 风控/WAF 拦截 → 中等冷却并切号（不判会话死亡）
	ErrorKindTransient   ErrorKind = "transient"    // 网络异常、超时或 5xx → 可退避重试与切号
	ErrorKindClient      ErrorKind = "client"       // 4xx 参数错误等 → 直接返回客户端
)

// UpstreamError 封装分类后的上游调用错误。
type UpstreamError struct {
	HTTPStatus int
	BizCode    int64
	Kind       ErrorKind
	Message    string
	RetryAfter time.Duration
}

func (e *UpstreamError) Error() string {
	if e.BizCode != 0 {
		return fmt.Sprintf("上游错误 [%s] (HTTP %d, code=%d): %s", e.Kind, e.HTTPStatus, e.BizCode, e.Message)
	}
	if e.HTTPStatus > 0 {
		return fmt.Sprintf("上游错误 [%s] (HTTP %d): %s", e.Kind, e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("上游错误 [%s]: %s", e.Kind, e.Message)
}

// ShouldFailover 判断该错误是否应当切换其他可用账号重试。
func (e *UpstreamError) ShouldFailover() bool {
	switch e.Kind {
	case ErrorKindSessionDead, ErrorKindHardCredit, ErrorKindSoftRate, ErrorKindRiskControl, ErrorKindTransient:
		return true
	default:
		return false
	}
}

// IsDeadBizCode 判断业务码是否为登录会话失效码。
func IsDeadBizCode(code int64) bool {
	return code == BizCodeNotLoggedIn || code == BizCodeAuthExpired
}

// errorPhraseRule 定义按响应文本关键词命中后的分级规则。
type errorPhraseRule struct {
	kind       ErrorKind
	httpStatus int
	phrases    []string
}

var errorClassifierTable = []errorPhraseRule{
	// 软限流规则必须排在 hard_credit 之前：避免 "rate limit reached" 等限流文案
	// 命中 hard_credit 的 "limit reached" 短语，导致账号被错误地长冷却 30 分钟。
	{
		kind:       ErrorKindSoftRate,
		httpStatus: http.StatusTooManyRequests,
		phrases: []string{
			"rate limit", "too many requests", "throttl", "qps exceed",
			"请求过于频繁", "限流", "触发流控", "请求太快",
		},
	},
	{
		kind:       ErrorKindHardCredit,
		httpStatus: http.StatusPaymentRequired,
		phrases: []string{
			"insufficient credit", "no credit", "credit exhausted", "out of credit",
			"quota exceeded", "quota exhaust", "payment required", "not enough credit",
			"insufficient balance", "balance not enough", "limit reached", "usage limit",
			"余额不足", "额度不足", "积分不足", "积分用完", "额度用尽", "没有积分", "配额不足",
		},
	},
	{
		kind:       ErrorKindSessionDead,
		httpStatus: http.StatusUnauthorized,
		phrases: []string{
			"not logged in", "unauthorized", "invalid token", "token expired",
			"login required", "session expired", "offline user session", "auth.inject failed",
			"未登录", "登录已过期", "请先登录", "token 失效", "登录失效",
		},
	},
}

// containsSessionDeadPhrase 检查上游文本中是否包含会话失效类提示语。
func containsSessionDeadPhrase(text string) bool {
	lower := strings.ToLower(text)
	for _, rule := range errorClassifierTable {
		if rule.kind != ErrorKindSessionDead {
			continue
		}
		for _, phrase := range rule.phrases {
			if strings.Contains(lower, phrase) {
				return true
			}
		}
	}
	return false
}

// ClassifyError 根据 HTTP 状态码、上游业务码、响应文本和 Retry-After 请求头完成分级分类。
func ClassifyError(httpStatus int, bizCode int64, body string, retryAfter time.Duration) *UpstreamError {
	msg := truncateText(strings.TrimSpace(body), 320)
	if msg == "" {
		msg = http.StatusText(httpStatus)
	}

	if IsDeadBizCode(bizCode) {
		return &UpstreamError{
			HTTPStatus: http.StatusUnauthorized,
			BizCode:    bizCode,
			Kind:       ErrorKindSessionDead,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	}

	// HTTP 429 语义明确：无论响应体文案如何，一律按软限流短冷却处理，先于短语匹配
	if httpStatus == http.StatusTooManyRequests {
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindSoftRate,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	}

	lower := strings.ToLower(body)
	for _, rule := range errorClassifierTable {
		for _, phrase := range rule.phrases {
			if strings.Contains(lower, phrase) {
				return &UpstreamError{
					HTTPStatus: rule.httpStatus,
					BizCode:    bizCode,
					Kind:       rule.kind,
					Message:    msg,
					RetryAfter: retryAfter,
				}
			}
		}
	}

	switch {
	case httpStatus == http.StatusUnauthorized:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindSessionDead,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	case httpStatus == http.StatusForbidden:
		// 403 多为 WAF/风控拦截（与 IP/指纹相关），令牌本身仍有效：中等冷却并切号，不判会话死亡
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindRiskControl,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	case httpStatus == http.StatusPaymentRequired:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindHardCredit,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	case httpStatus >= 500 || httpStatus == http.StatusRequestTimeout || httpStatus == 0:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindTransient,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	default:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindClient,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	}
}

// ParseRetryAfter 解析 HTTP Retry-After 响应头（支持秒数和 RFC1123 日期格式）。
func ParseRetryAfter(headerVal string) time.Duration {
	v := strings.TrimSpace(headerVal)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}

// DetectEnvelopeError 检测 JSON 业务信封 {"code": 非0, "msg": ...}。
func DetectEnvelopeError(body []byte) (int64, string, bool) {
	var env struct {
		Code    *int64 `json:"code"`
		Msg     string `json:"msg"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return 0, "", false
	}
	if env.Code == nil || *env.Code == 0 {
		return 0, "", false
	}
	msg := strings.TrimSpace(env.Msg)
	if msg == "" {
		msg = strings.TrimSpace(env.Message)
	}
	return *env.Code, msg, true
}

func truncateText(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "..."
}
