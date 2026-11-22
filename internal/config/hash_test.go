package config

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestPanelPasswordHashPBKDF2AndLegacy(t *testing.T) {
	hash, err := HashPanelPassword("my-secret-pass")
	if err != nil {
		t.Fatal(err)
	}
	if !IsPBKDF2Hash(hash) {
		t.Fatalf("新哈希应为 pbkdf2-sha256 格式: %s", hash)
	}
	if !VerifyPanelPassword("my-secret-pass", hash) {
		t.Fatal("PBKDF2 哈希应校验通过")
	}
	if VerifyPanelPassword("wrong-pass", hash) {
		t.Fatal("错误密码不应校验通过")
	}

	// v1.0.1 及更早的单轮 SHA-256 旧格式必须保持兼容
	salt := "deadbeefdeadbeef"
	sum := sha256.Sum256([]byte(salt + ":legacy-pass"))
	legacy := salt + "$" + hex.EncodeToString(sum[:])
	if IsPBKDF2Hash(legacy) {
		t.Fatal("旧格式不应被识别为 PBKDF2")
	}
	if !VerifyPanelPassword("legacy-pass", legacy) {
		t.Fatal("旧格式哈希应保持兼容可校验")
	}
	if VerifyPanelPassword("legacy-wrong", legacy) {
		t.Fatal("旧格式错误密码不应通过")
	}
}

func TestGenerateVKeyCharsetAndLength(t *testing.T) {
	key := GenerateVKey()
	if !strings.HasPrefix(key, "vKey-") {
		t.Fatalf("密钥应以 vKey- 开头: %s", key)
	}
	body := key[len("vKey-"):]
	if len(body) != 28 {
		t.Fatalf("密钥体应为 28 位，实际 %d", len(body))
	}
	for _, r := range body {
		if !strings.ContainsRune(vKeyAlphabet, r) {
			t.Fatalf("密钥含非法字符 %q", r)
		}
	}
	seen := make(map[byte]struct{})
	for i := 0; i < 500; i++ {
		k := GenerateVKey()
		seen[k[5]] = struct{}{}
	}
	if len(seen) < 30 {
		t.Fatalf("首位字符分布应覆盖大部分字母表（无模偏差），实际仅 %d 种", len(seen))
	}
}
