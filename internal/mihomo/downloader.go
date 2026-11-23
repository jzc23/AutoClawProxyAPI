package mihomo

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	// DefaultVersion 为一键在线下载 mihomo 内核的默认版本（支持环境变量 MIHOMO_VERSION 覆盖）。
	DefaultVersion = "v1.19.29"
	// DefaultMirrorPrefix 为国内直连加速下载前缀（失败时自动回落直连 GitHub）。
	DefaultMirrorPrefix = "https://ghfast.top/"
	// DefaultGitHubReleaseBase 为官方 Release 下载根地址。
	DefaultGitHubReleaseBase = "https://github.com/MetaCubeX/mihomo/releases/download"
)

// DownloadState 实时反映 mihomo 内核在线下载进度与结果。
type DownloadState struct {
	Status          string `json:"status"` // idle | downloading | done | error
	Version         string `json:"version"`
	Asset           string `json:"asset,omitempty"`
	Source          string `json:"source,omitempty"` // mirror | direct
	DownloadedBytes int64  `json:"downloaded_bytes"`
	TotalBytes      int64  `json:"total_bytes"`
	Percent         int    `json:"percent"`
	Message         string `json:"message,omitempty"`
	Error           string `json:"error,omitempty"`
	UpdatedAt       string `json:"updated_at,omitempty"`
}

// ResolveVersion 读取 MIHOMO_VERSION 环境变量，未设置时返回默认版本 v1.19.29。
func ResolveVersion() string {
	if v := strings.TrimSpace(os.Getenv("MIHOMO_VERSION")); v != "" {
		if !strings.HasPrefix(v, "v") && !strings.HasPrefix(v, "V") {
			return "v" + v
		}
		return "v" + strings.TrimPrefix(strings.TrimPrefix(v, "v"), "V")
	}
	return DefaultVersion
}

// BinaryName 返回目标操作系统对应的 mihomo 可执行文件名。
func BinaryName(goos string) string {
	if strings.EqualFold(strings.TrimSpace(goos), "windows") {
		return "mihomo.exe"
	}
	return "mihomo"
}

// AssetName 根据 GOOS/GOARCH 与版本号拼接官方 Release 压缩包文件名及归档类型（zip / gz）。
func AssetName(goos, goarch, ver string) (asset string, archiveType string, err error) {
	osName := strings.ToLower(strings.TrimSpace(goos))
	archName := strings.ToLower(strings.TrimSpace(goarch))
	version := strings.TrimSpace(ver)
	if version == "" {
		version = DefaultVersion
	}

	switch archName {
	case "amd64", "arm64", "386":
	case "arm":
		archName = "armv7"
	default:
		return "", "", fmt.Errorf("暂不支持当前 CPU 架构: %s", goarch)
	}

	switch osName {
	case "windows":
		return fmt.Sprintf("mihomo-windows-%s-%s.zip", archName, version), "zip", nil
	case "linux", "darwin":
		return fmt.Sprintf("mihomo-%s-%s-%s.gz", osName, archName, version), "gz", nil
	default:
		return "", "", fmt.Errorf("暂不支持当前操作系统: %s", goos)
	}
}

// BuildDownloadURLs 生成「国内加速镜像优先 + GitHub 官方直连兜底」两条下载地址。
func BuildDownloadURLs(goos, goarch, ver, mirrorPrefix, githubBase string) (mirrorURL, directURL, asset, archiveType string, err error) {
	asset, archiveType, err = AssetName(goos, goarch, ver)
	if err != nil {
		return "", "", "", "", err
	}
	base := strings.TrimRight(strings.TrimSpace(githubBase), "/")
	if base == "" {
		base = DefaultGitHubReleaseBase
	}
	directURL = fmt.Sprintf("%s/%s/%s", base, ver, asset)

	mp := strings.TrimSpace(mirrorPrefix)
	if mp == "" {
		mp = DefaultMirrorPrefix
	}
	if !strings.HasSuffix(mp, "/") {
		mp += "/"
	}
	mirrorURL = mp + directURL
	return mirrorURL, directURL, asset, archiveType, nil
}

// DetectBinary 自动探测程序目录下是否已存在 mihomo（Windows 为 mihomo.exe）。
func DetectBinary(baseDir string) (path string, exists bool) {
	binName := BinaryName(runtime.GOOS)
	candidates := make([]string, 0, 3)
	if strings.TrimSpace(baseDir) != "" {
		candidates = append(candidates, filepath.Join(baseDir, binName))
	}
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates, filepath.Join(exeDir, binName))
	}
	candidates = append(candidates, binName)

	seen := make(map[string]struct{}, len(candidates))
	for _, c := range candidates {
		abs, err := filepath.Abs(c)
		if err != nil {
			abs = c
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		if st, err := os.Stat(abs); err == nil && !st.IsDir() && st.Size() > 0 {
			return abs, true
		}
	}
	target := filepath.Join(baseDir, binName)
	if abs, err := filepath.Abs(target); err == nil {
		target = abs
	}
	return target, false
}

// downloadAndExtract 负责下载（镜像优先、直连兜底）→ .part 落盘 → 纯标准库解压至 .tmp → os.Rename 原子替换。
func downloadAndExtract(
	ctx context.Context,
	client *http.Client,
	mirrorURL, directURL, archiveType, targetPath string,
	onProgress func(source string, downloaded, total int64),
) (usedSource string, err error) {
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Minute}
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return "", fmt.Errorf("创建目标目录失败: %w", err)
	}

	partPath := targetPath + ".part"
	tmpPath := targetPath + ".tmp"
	cleanup := func() {
		_ = os.Remove(partPath)
		_ = os.Remove(tmpPath)
	}
	defer func() {
		if err != nil {
			cleanup()
		}
	}()

	type sourceCand struct {
		name string
		url  string
	}
	sources := []sourceCand{
		{name: "mirror", url: mirrorURL},
		{name: "direct", url: directURL},
	}

	var lastErr error
	downloadedOK := false
	for _, src := range sources {
		if strings.TrimSpace(src.url) == "" {
			continue
		}
		_ = os.Remove(partPath)
		if errDL := downloadToFile(ctx, client, src.url, partPath, func(got, total int64) {
			if onProgress != nil {
				onProgress(src.name, got, total)
			}
		}); errDL != nil {
			lastErr = fmt.Errorf("%s 下载失败: %w", src.name, errDL)
			continue
		}
		usedSource = src.name
		downloadedOK = true
		break
	}
	if !downloadedOK {
		if lastErr == nil {
			lastErr = errors.New("未配置有效下载地址")
		}
		return "", lastErr
	}

	switch archiveType {
	case "zip":
		if err = extractZipBinary(partPath, tmpPath); err != nil {
			return usedSource, fmt.Errorf("解压 ZIP 归档失败: %w", err)
		}
	case "gz":
		if err = extractGzipBinary(partPath, tmpPath); err != nil {
			return usedSource, fmt.Errorf("解压 GZ 归档失败: %w", err)
		}
	default:
		return usedSource, fmt.Errorf("不支持的归档格式: %s", archiveType)
	}

	// Linux / macOS 赋予可执行权限
	if runtime.GOOS != "windows" {
		_ = os.Chmod(tmpPath, 0o755)
	}

	// 原子替换目标可执行文件
	if err = os.Rename(tmpPath, targetPath); err != nil {
		// Windows 上覆盖已存在文件前先尝试移除旧文件
		_ = os.Remove(targetPath)
		if err2 := os.Rename(tmpPath, targetPath); err2 != nil {
			return usedSource, fmt.Errorf("原子落盘内核文件失败: %w", err2)
		}
	}
	_ = os.Remove(partPath)
	return usedSource, nil
}

func downloadToFile(ctx context.Context, client *http.Client, rawURL, partPath string, onProgress func(downloaded, total int64)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "AutoClaw2API/1.1.0")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	total := resp.ContentLength
	var downloaded int64
	buf := make([]byte, 32*1024)
	lastReport := time.Time{}

	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, wErr := f.Write(buf[:n]); wErr != nil {
				return wErr
			}
			downloaded += int64(n)
			if onProgress != nil && (lastReport.IsZero() || time.Since(lastReport) >= 120*time.Millisecond) {
				onProgress(downloaded, total)
				lastReport = time.Now()
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if downloaded == 0 {
		return errors.New("下载内容为空")
	}
	if onProgress != nil {
		onProgress(downloaded, total)
	}
	return f.Sync()
}

func extractZipBinary(zipPath, dstPath string) error {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer func() { _ = zr.Close() }()

	var picked *zip.File
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		low := strings.ToLower(filepath.Base(f.Name))
		if strings.HasSuffix(low, ".exe") || strings.Contains(low, "mihomo") {
			picked = f
			break
		}
	}
	if picked == nil {
		return errors.New("ZIP 压缩包中未找到 mihomo 可执行文件")
	}

	rc, err := picked.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	out, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	written, err := io.Copy(out, rc)
	if err != nil {
		return err
	}
	if written == 0 {
		return errors.New("解压后的 mihomo 文件为空")
	}
	return out.Sync()
}

func extractGzipBinary(gzPath, dstPath string) error {
	in, err := os.Open(gzPath)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	gr, err := gzip.NewReader(in)
	if err != nil {
		return err
	}
	defer func() { _ = gr.Close() }()

	out, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	written, err := io.Copy(out, gr)
	if err != nil {
		return err
	}
	if written == 0 {
		return errors.New("解压后的 mihomo 文件为空")
	}
	return out.Sync()
}
