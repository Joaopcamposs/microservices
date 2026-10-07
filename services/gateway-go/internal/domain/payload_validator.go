package domain

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// ErrUnsupportedJobType indica que o tipo existe no contrato mas ainda não tem schema.
var ErrUnsupportedJobType = errors.New("tipo de job sem schema")

// ErrInvalidPayload indica que o payload viola o JSON Schema do tipo. A mensagem carrega
// todas as violações.
var ErrInvalidPayload = errors.New("payload inválido")

// PayloadValidator valida o payload contra contracts/jobs/<type>.schema.json.
//
// A fonte da verdade é o schema compartilhado, não código Go: o gateway Python lê os mesmos
// arquivos, então os dois aceitam e rejeitam exatamente os mesmos pedidos.
type PayloadValidator struct {
	schemas map[JobType]*jsonschema.Schema
}

// NewPayloadValidator carrega os schemas existentes em dir e ignora os tipos sem arquivo.
// Schema quebrado derruba a subida do serviço em vez de falhar só na primeira requisição.
func NewPayloadValidator(dir string) (*PayloadValidator, error) {
	compiler := jsonschema.NewCompiler()
	schemas := make(map[JobType]*jsonschema.Schema)
	for _, jobType := range AllJobTypes {
		path := filepath.Join(dir, string(jobType)+".schema.json")
		if _, err := os.Stat(path); err != nil {
			continue
		}
		schema, err := compiler.Compile(path)
		if err != nil {
			return nil, fmt.Errorf("compilar %s: %w", path, err)
		}
		schemas[jobType] = schema
	}
	return &PayloadValidator{schemas: schemas}, nil
}

// Validate confere o payload (JSON bruto) contra o schema do tipo. Devolve
// ErrUnsupportedJobType se o tipo não tem schema e ErrInvalidPayload (embrulhado, com os
// detalhes) se o payload não confere. Números são lidos como json.Number para o schema
// distinguir inteiro de decimal sem perder precisão.
func (v *PayloadValidator) Validate(jobType JobType, payload []byte) error {
	schema, ok := v.schemas[jobType]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnsupportedJobType, jobType)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if err := schema.Validate(instance); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	return nil
}
