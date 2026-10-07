package domain

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// pbkdf2DigestBytes é o tamanho do digest em bytes; fixo no contrato para o resultado ser
// comparável com as outras stacks.
const pbkdf2DigestBytes = 32

// handleCPUPBKDF2 deriva a chave PBKDF2-HMAC-SHA256. Roda na goroutine do pool: o scheduler do
// Go espalha as goroutines pelos núcleos, então não precisa de thread ou processo à parte.
func handleCPUPBKDF2(_ context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var input struct {
		Password   string `json:"password"`
		Salt       string `json:"salt"`
		Iterations int    `json:"iterations"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("payload cpu.pbkdf2: %w", err)
	}
	key, err := pbkdf2.Key(sha256.New, input.Password, []byte(input.Salt), input.Iterations, pbkdf2DigestBytes)
	if err != nil {
		return nil, fmt.Errorf("derivar chave: %w", err)
	}
	return json.Marshal(struct {
		Digest string `json:"digest"`
	}{Digest: hex.EncodeToString(key)})
}
