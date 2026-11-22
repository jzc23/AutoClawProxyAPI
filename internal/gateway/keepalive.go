package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"autoclaw2api/internal/protocol"
)

// 非流式 JSON 响应的空白心跳（keepalive）参数。
//
// 非流式请求要等上游完整生成结束才有响应体，中间是数十秒到几分钟的零字节空窗，
// nginx / 雷池 / Lucky / Cloudflare（默认 100s）等反向代理会按读超时切断连接返回 504。
//
// 机制：宽限期后先写出首个空格并 flush（此时响应头已隐式提交为 200），之后按固定间隔续写；
// JSON 规范（RFC 8259）允许值之前有任意空白，因此心跳字节不影响客户端解析，
// 但每次写出都会重置代理的读超时计时器，保证传输不被切断。
//
// 代价：心跳提交后再发生的错误无法变更状态码，只能以 HTTP 200 + 协议错误 JSON 体收尾；
// 宽限期内完成/失败的请求仍返回真实状态码。
var (
	keepaliveGrace    = 10 * time.Second
	keepaliveInterval = 15 * time.Second
)

// jsonKeepalive 为非流式 JSON 响应提供空白心跳保活。
type jsonKeepalive struct {
	mu        sync.Mutex
	w         http.ResponseWriter
	flusher   http.Flusher
	stop      chan struct{}
	done      chan struct{}
	committed bool
}

func newJSONKeepalive(w http.ResponseWriter) *jsonKeepalive {
	flusher, _ := w.(http.Flusher)
	return &jsonKeepalive{
		w:       w,
		flusher: flusher,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// start 启动心跳协程：宽限期后写出首个空白，之后按固定间隔续写；客户端断开时自动停止。
func (k *jsonKeepalive) start() {
	go func() {
		defer close(k.done)
		timer := time.NewTimer(keepaliveGrace)
		defer timer.Stop()
		for {
			select {
			case <-k.stop:
				return
			case <-timer.C:
				if !k.writeSpace() {
					return
				}
				timer.Reset(keepaliveInterval)
			}
		}
	}()
}

func (k *jsonKeepalive) writeSpace() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	// 客户端已断开时 Write 报错，直接停止心跳
	if _, err := k.w.Write([]byte(" ")); err != nil {
		return false
	}
	if k.flusher != nil {
		k.flusher.Flush()
	}
	k.committed = true
	return true
}

// Stop 停止心跳并等待协程退出，返回是否已提交过心跳字节。
func (k *jsonKeepalive) Stop() bool {
	close(k.stop)
	<-k.done
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.committed
}

// startNonStreamKeepalive 为非流式 JSON 响应预置响应头并启动空白心跳。
func startNonStreamKeepalive(w http.ResponseWriter) *jsonKeepalive {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// 禁用 nginx 等对响应的缓冲：否则心跳字节会被代理攒住，客户端仍看不到数据
	w.Header().Set("X-Accel-Buffering", "no")
	ka := newJSONKeepalive(w)
	ka.start()
	return ka
}

// finishNonStreamKeepalive 停止心跳并写出最终 JSON 体；已提交心跳时状态码固定为 200。
func finishNonStreamKeepalive(w http.ResponseWriter, ka *jsonKeepalive, status int, payload []byte) {
	committed := ka.Stop()
	if !committed {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
	}
	_, _ = w.Write(payload)
}

// stopAndReportError 停止心跳；已提交心跳时以 200 + 协议错误 JSON 收尾并返回 true，
// 未提交时返回 false（由调用方按原逻辑用真实状态码返回错误）。
func stopAndReportError(w http.ResponseWriter, ka *jsonKeepalive, shape string, err error) bool {
	committed := ka.Stop()
	if !committed {
		return false
	}
	message := err.Error()
	errType := "upstream_error"
	var uerr *protocol.UpstreamError
	if errors.As(err, &uerr) {
		message = uerr.Message
		errType = string(uerr.Kind)
	}
	var payload any
	switch shape {
	case "anthropic":
		payload = map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "api_error",
				"message": message,
			},
		}
	default:
		payload = map[string]any{
			"error": map[string]any{
				"message": message,
				"type":    errType,
			},
		}
	}
	data, _ := json.Marshal(payload)
	_, _ = w.Write(data)
	return true
}
