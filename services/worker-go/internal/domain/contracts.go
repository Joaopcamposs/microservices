package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// ErrInvalidMessage marca a mensagem que nunca vai passar: JSON quebrado, envelope fora do
// contrato, payload inválido ou tipo sem handler. Reentregar não adianta, então ela segue
// para a DLQ.
var ErrInvalidMessage = errors.New("mensagem inválida")

// ContractValidator converte o corpo bruto da mensagem em Job aplicando envelope e payload
// schema.
//
// A fonte da verdade são os arquivos de contracts/, os mesmos que gateways e o worker Python
// usam: o worker rejeita exatamente o que o contrato rejeita, sem regras duplicadas em Go.
type ContractValidator struct {
	envelope *jsonschema.Schema
	payloads map[JobType]*jsonschema.Schema
}

// NewContractValidator carrega envelope.schema.json e os jobs/<type>.schema.json existentes.
// Schema quebrado derruba a subida em vez de rejeitar mensagens boas em runtime.
func NewContractValidator(contractsDir string) (*ContractValidator, error) {
	compiler := jsonschema.NewCompiler()
	envelope, err := compiler.Compile(filepath.Join(contractsDir, "envelope.schema.json"))
	if err != nil {
		return nil, fmt.Errorf("compilar envelope: %w", err)
	}
	payloads := make(map[JobType]*jsonschema.Schema)
	for _, jobType := range AllJobTypes {
		path := filepath.Join(contractsDir, "jobs", string(jobType)+".schema.json")
		if _, err := os.Stat(path); err != nil {
			continue
		}
		schema, err := compiler.Compile(path)
		if err != nil {
			return nil, fmt.Errorf("compilar %s: %w", path, err)
		}
		payloads[jobType] = schema
	}
	return &ContractValidator{envelope: envelope, payloads: payloads}, nil
}

// Parse valida o corpo e devolve o Job; o erro embrulha ErrInvalidMessage com o motivo.
func (v *ContractValidator) Parse(body []byte) (Job, error) {
	if err := validateBytes(v.envelope, body); err != nil {
		return Job{}, fmt.Errorf("%w: envelope: %v", ErrInvalidMessage, err)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		return Job{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	schema, ok := v.payloads[job.Type]
	if !ok {
		return Job{}, fmt.Errorf("%w: tipo sem schema de payload: %s", ErrInvalidMessage, job.Type)
	}
	if err := validateBytes(schema, job.Payload); err != nil {
		return Job{}, fmt.Errorf("%w: payload: %v", ErrInvalidMessage, err)
	}
	return job, nil
}

// validateBytes valida JSON bruto contra o schema. Números viram json.Number para o schema
// distinguir inteiro de decimal sem perder precisão.
func validateBytes(schema *jsonschema.Schema, raw []byte) error {
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	return schema.Validate(instance)
}
