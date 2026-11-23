package gateway

import (
	"errors"
	"io"
	"net/http"

	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/translator"
)

// handleAnthropicMessages 实现 Anthropic Messages API（POST /v1/messages），可直接接入 Claude Code 等客户端。
func (s *Server) handleAnthropicMessages(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "读取请求体失败: "+err.Error())
		return
	}

	chatBody, model, stream, err := translator.AnthropicToOpenAIRequest(rawBody)
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if model == "" {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "缺少必需字段 model")
		return
	}

	stickyKey := deriveStickySessionKey(model, rawBody, chatBody)

	if stream {
		resp, _, err := s.executeChatStreamWithFailover(r.Context(), model, chatBody, stickyKey)
		if err != nil {
			writeAnthropicUpstreamError(w, err)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		flusher, _ := w.(http.Flusher)
		conv := translator.NewAnthropicStreamConverter(model)
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
		if stopAndReportError(w, ka, "anthropic", err) {
			return
		}
		writeAnthropicUpstreamError(w, err)
		return
	}
	converted, err := translator.OpenAIToAnthropicResponse(payload, model)
	if err != nil {
		if stopAndReportError(w, ka, "anthropic", errors.New("转换 Anthropic 响应失败: "+err.Error())) {
			return
		}
		writeAnthropicError(w, http.StatusBadGateway, "api_error", "转换 Anthropic 响应失败: "+err.Error())
		return
	}
	finishNonStreamKeepalive(w, ka, http.StatusOK, converted)
}

func writeAnthropicError(w http.ResponseWriter, status int, errType, message string) {
	writeJSON(w, status, map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    errType,
			"message": message,
		},
	})
}

func writeAnthropicUpstreamError(w http.ResponseWriter, err error) {
	var uerr *protocol.UpstreamError
	if errors.As(err, &uerr) {
		status := uerr.HTTPStatus
		if status <= 0 {
			status = http.StatusBadGateway
		}
		errType := "api_error"
		switch uerr.Kind {
		case protocol.ErrorKindSessionDead:
			errType = "authentication_error"
		case protocol.ErrorKindSoftRate:
			errType = "rate_limit_error"
		case protocol.ErrorKindClient:
			errType = "invalid_request_error"
		}
		writeAnthropicError(w, status, errType, uerr.Message)
		return
	}
	writeAnthropicError(w, http.StatusBadGateway, "api_error", err.Error())
}
