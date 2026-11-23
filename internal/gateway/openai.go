package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/translator"
)

// handleListModels 实现 OpenAI 兼容的 GET /v1/models。
func (s *Server) handleListModels(w http.ResponseWriter, _ *http.Request) {
	items := s.router.ListOpenAIModels()
	now := time.Now().Unix()
	data := make([]map[string]any, 0, len(items))
	for _, m := range items {
		owner := "autoclaw2api"
		if m.Channel != "" {
			owner = m.Channel
		}
		data = append(data, map[string]any{
			"id":       m.ID,
			"object":   "model",
			"created":  now,
			"owned_by": owner,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   data,
	})
}

// handleChatCompletions 实现 OpenAI 兼容的 POST /v1/chat/completions（流式与非流式）。
func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "读取请求体失败: "+err.Error())
		return
	}

	var meta struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(rawBody, &meta); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "请求体不是合法 JSON: "+err.Error())
		return
	}
	if strings.TrimSpace(meta.Model) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "缺少必需字段 model")
		return
	}

	stickyKey := deriveStickySessionKey(meta.Model, rawBody, rawBody)

	if meta.Stream {
		resp, usedAcc, err := s.executeChatStreamWithFailover(r.Context(), meta.Model, rawBody, stickyKey)
		if err != nil {
			writeOpenAIUpstreamError(w, err)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		s.streamOpenAIPassthrough(w, resp.Body, usedAcc)
		return
	}

	// 非流式：启用空白心跳保活，长生成期间持续重置反向代理/Cloudflare 的读超时，避免连接被切断返回 504
	ka := startNonStreamKeepalive(w)
	payload, _, err := s.executeChatWithFailover(r.Context(), meta.Model, rawBody, stickyKey)
	if err != nil {
		if stopAndReportError(w, ka, "openai", err) {
			return
		}
		writeOpenAIUpstreamError(w, err)
		return
	}
	finishNonStreamKeepalive(w, ka, http.StatusOK, payload)
}

// handleResponses 实现 OpenAI 兼容的 POST /v1/responses（含非流式与流式事件转换）。
func (s *Server) handleResponses(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "读取请求体失败: "+err.Error())
		return
	}

	chatBody, model, stream, err := translator.ResponsesToOpenAIRequest(rawBody)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if model == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "缺少必需字段 model")
		return
	}

	stickyKey := deriveStickySessionKey(model, rawBody, chatBody)

	if stream {
		resp, _, err := s.executeChatStreamWithFailover(r.Context(), model, chatBody, stickyKey)
		if err != nil {
			writeOpenAIUpstreamError(w, err)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		flusher, _ := w.(http.Flusher)
		conv := translator.NewResponsesStreamConverter(model)
		scanSSEData(resp.Body, func(data []byte) bool {
			for _, frame := range conv.ProcessSSEData(data) {
				if _, err := w.Write(frame); err != nil {
					return false
				}
			}
			if flusher != nil {
				flusher.Flush()
			}
			return true
		})
		for _, frame := range conv.Finish() {
			_, _ = w.Write(frame)
		}
		if flusher != nil {
			flusher.Flush()
		}
		return
	}

	// 非流式：启用空白心跳保活，长生成期间持续重置反向代理/Cloudflare 的读超时，避免连接被切断返回 504
	ka := startNonStreamKeepalive(w)
	payload, _, err := s.executeChatWithFailover(r.Context(), model, chatBody, stickyKey)
	if err != nil {
		if stopAndReportError(w, ka, "openai", err) {
			return
		}
		writeOpenAIUpstreamError(w, err)
		return
	}
	converted, err := translator.OpenAIToResponsesResponse(payload, model)
	if err != nil {
		if stopAndReportError(w, ka, "openai", errors.New("转换 Responses 响应失败: "+err.Error())) {
			return
		}
		writeOpenAIError(w, http.StatusBadGateway, "server_error", "转换 Responses 响应失败: "+err.Error())
		return
	}
	finishNonStreamKeepalive(w, ka, http.StatusOK, converted)
}

func (s *Server) streamOpenAIPassthrough(w http.ResponseWriter, body io.Reader, _ *accounts.Account) {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	reader := bufio.NewReaderSize(body, 32*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if _, wErr := w.Write(line); wErr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

// executeChatWithFailover 执行带短时会话粘性、指数退避与自动切号故障转移的非流式对话请求。
func (s *Server) executeChatWithFailover(ctx context.Context, requestedModel string, chatBody []byte, stickyKey string) ([]byte, *accounts.Account, error) {
	targets, err := s.router.ResolveTargets(requestedModel)
	if err != nil {
		return nil, nil, &protocol.UpstreamError{
			HTTPStatus: http.StatusBadRequest,
			Kind:       protocol.ErrorKindClient,
			Message:    err.Error(),
		}
	}

	retryCfg := s.cfgMgr.Get().Retry
	maxAttempts := max(1, retryCfg.MaxRetries+1)
	triedAccounts := make(map[string]bool)
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		acc, target, ok := s.pickNextAccount(targets, stickyKey, triedAccounts)
		if !ok {
			if lastErr != nil {
				return nil, nil, lastErr
			}
			return nil, nil, &protocol.UpstreamError{
				HTTPStatus: http.StatusServiceUnavailable,
				Kind:       protocol.ErrorKindTransient,
				Message:    fmt.Sprintf("模型 %s 暂无可用就绪账号（请检查账号池状态或稍后重试）", requestedModel),
			}
		}
		triedAccounts[acc.ID] = true

		baseURL := s.baseURLFor(target.Region)
		client, errClient := s.proxyMgr.ClientForAccount(acc.ID, string(target.Region), 10*time.Minute)
		if errClient != nil {
			lastErr = &protocol.UpstreamError{
				HTTPStatus: http.StatusBadGateway,
				Kind:       protocol.ErrorKindTransient,
				Message:    "代理出口不可用: " + errClient.Error(),
			}
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] 代理出口不可用，自动切号: %v", acc.Cred.DisplayLabel(), errClient)
			}
			continue
		}
		respBytes, errCall := protocol.ChatCompletion(ctx, client, protocol.ChatRequest{
			BaseURL: baseURL,
			Token:   acc.Cred.AccessToken,
			Model:   target.UpstreamModel,
			Body:    chatBody,
		})
		if errCall == nil {
			s.pool.ReportSuccess(acc.ID)
			return respBytes, acc, nil
		}

		lastErr = errCall
		var uerr *protocol.UpstreamError
		if errors.As(errCall, &uerr) {
			s.pool.ReportError(acc.ID, uerr)
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] (%s) 调用模型 %s 失败 [%s]: %s",
					acc.Cred.DisplayLabel(), target.Region, target.UpstreamModel, uerr.Kind, uerr.Message)
			}
			if !uerr.ShouldFailover() {
				return nil, nil, uerr
			}
			// 还有候选账号可切号时，限流/积分不足/失效立即切号；仅在瞬态网络错误或重试时短暂退避
			if attempt+1 < maxAttempts {
				if uerr.Kind == protocol.ErrorKindTransient {
					if !sleepWithContext(ctx, calcBackoff(attempt, retryCfg, uerr.RetryAfter)) {
						return nil, nil, ctx.Err()
					}
				}
				continue
			}
		}
		return nil, nil, lastErr
	}
	return nil, nil, lastErr
}

// executeChatStreamWithFailover 执行带短时会话粘性与自动切号故障转移的流式 SSE 请求。
func (s *Server) executeChatStreamWithFailover(ctx context.Context, requestedModel string, chatBody []byte, stickyKey string) (*http.Response, *accounts.Account, error) {
	targets, err := s.router.ResolveTargets(requestedModel)
	if err != nil {
		return nil, nil, &protocol.UpstreamError{
			HTTPStatus: http.StatusBadRequest,
			Kind:       protocol.ErrorKindClient,
			Message:    err.Error(),
		}
	}

	retryCfg := s.cfgMgr.Get().Retry
	maxAttempts := max(1, retryCfg.MaxRetries+1)
	triedAccounts := make(map[string]bool)
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		acc, target, ok := s.pickNextAccount(targets, stickyKey, triedAccounts)
		if !ok {
			if lastErr != nil {
				return nil, nil, lastErr
			}
			return nil, nil, &protocol.UpstreamError{
				HTTPStatus: http.StatusServiceUnavailable,
				Kind:       protocol.ErrorKindTransient,
				Message:    fmt.Sprintf("模型 %s 暂无可用就绪账号（请检查账号池状态或稍后重试）", requestedModel),
			}
		}
		triedAccounts[acc.ID] = true

		baseURL := s.baseURLFor(target.Region)
		client, errClient := s.proxyMgr.ClientForAccount(acc.ID, string(target.Region), 0)
		if errClient != nil {
			lastErr = &protocol.UpstreamError{
				HTTPStatus: http.StatusBadGateway,
				Kind:       protocol.ErrorKindTransient,
				Message:    "代理出口不可用: " + errClient.Error(),
			}
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] 流式代理出口不可用，自动切号: %v", acc.Cred.DisplayLabel(), errClient)
			}
			continue
		}
		resp, errCall := protocol.ChatCompletionStream(ctx, client, protocol.ChatRequest{
			BaseURL: baseURL,
			Token:   acc.Cred.AccessToken,
			Model:   target.UpstreamModel,
			Body:    chatBody,
		})
		if errCall == nil {
			s.pool.ReportSuccess(acc.ID)
			return resp, acc, nil
		}

		lastErr = errCall
		var uerr *protocol.UpstreamError
		if errors.As(errCall, &uerr) {
			s.pool.ReportError(acc.ID, uerr)
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] (%s) 流式调用模型 %s 失败 [%s]: %s",
					acc.Cred.DisplayLabel(), target.Region, target.UpstreamModel, uerr.Kind, uerr.Message)
			}
			if !uerr.ShouldFailover() {
				return nil, nil, uerr
			}
			if attempt+1 < maxAttempts {
				if uerr.Kind == protocol.ErrorKindTransient {
					if !sleepWithContext(ctx, calcBackoff(attempt, retryCfg, uerr.RetryAfter)) {
						return nil, nil, ctx.Err()
					}
				}
				continue
			}
		}
		return nil, nil, lastErr
	}
	return nil, nil, lastErr
}

func (s *Server) pickNextAccount(targets []accounts.RouteTarget, stickyKey string, exclude map[string]bool) (*accounts.Account, accounts.RouteTarget, bool) {
	if stickyKey != "" && len(targets) > 1 {
		if stickyRegion, ok := s.pool.StickyAccountRegion(stickyKey, exclude); ok {
			for _, target := range targets {
				if target.Region == stickyRegion {
					if acc, acquired := s.pool.AcquireSticky(target.Region, stickyKey, exclude); acquired {
						return acc, target, true
					}
				}
			}
		}
	}
	for _, target := range targets {
		if acc, ok := s.pool.AcquireSticky(target.Region, stickyKey, exclude); ok {
			return acc, target, true
		}
	}
	return nil, accounts.RouteTarget{}, false
}

// scanSSEData 逐行读取上游 SSE 数据流并回调每条 data 载荷。
func scanSSEData(body io.Reader, onData func([]byte) bool) {
	reader := bufio.NewReaderSize(body, 64*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimSpace(line)
			if bytes.HasPrefix(trimmed, []byte("data:")) {
				payload := bytes.TrimSpace(trimmed[5:])
				if len(payload) > 0 {
					if !onData(payload) {
						return
					}
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func writeOpenAIError(w http.ResponseWriter, status int, errType, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errType,
		},
	})
}

func writeOpenAIUpstreamError(w http.ResponseWriter, err error) {
	var uerr *protocol.UpstreamError
	if errors.As(err, &uerr) {
		status := uerr.HTTPStatus
		if status <= 0 {
			status = http.StatusBadGateway
		}
		writeOpenAIError(w, status, string(uerr.Kind), uerr.Message)
		return
	}
	writeOpenAIError(w, http.StatusBadGateway, "upstream_error", err.Error())
}
