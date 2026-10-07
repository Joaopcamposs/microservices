package domain

import (
	"context"
	"encoding/json"
	"fmt"
)

// Constantes do LCG de 64 bits (Knuth MMIX) que gera os registros. Um gerador próprio, e não
// math/rand, garante a mesma sequência em Go e em Python.
const (
	lcgMultiplier uint64 = 6364136223846793005
	lcgIncrement  uint64 = 1442695040888963407
	groupCount           = 16
	valueRange           = 100000
)

// record é um registro gerado; a ordem dos campos dá o mesmo JSON compacto do Python.
type record struct {
	ID    int    `json:"id"`
	Group string `json:"group"`
	Value uint64 `json:"value"`
	Tag   string `json:"tag"`
}

// groupTotals acumula contagem e soma de um grupo.
type groupTotals struct {
	Count int    `json:"count"`
	Sum   uint64 `json:"sum"`
}

// generateRecords gera `count` registros determinísticos a partir de `seed`.
func generateRecords(count int, seed uint64) []record {
	state := seed
	next := func() uint64 {
		state = state*lcgMultiplier + lcgIncrement // estoura em uint64 de propósito: é o módulo 2^64
		return state
	}
	records := make([]record, count)
	for i := range records {
		group := (next() >> 33) % groupCount
		value := (next() >> 33) % valueRange
		tag := next() >> 40
		records[i] = record{ID: i, Group: fmt.Sprintf("g%d", group), Value: value, Tag: fmt.Sprintf("t%06x", tag)}
	}
	return records
}

// handleDataJSONTransform gera, serializa, interpreta e agrega os registros por grupo. Serializar
// e interpretar de verdade é o que o job mede; `input_bytes` prova que o JSON intermediário foi
// idêntico ao das outras stacks (separadores compactos, mesma ordem de campos).
func handleDataJSONTransform(_ context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var input struct {
		Records int    `json:"records"`
		Seed    uint64 `json:"seed"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("payload data.json_transform: %w", err)
	}
	text, err := json.Marshal(generateRecords(input.Records, input.Seed))
	if err != nil {
		return nil, fmt.Errorf("serializar registros: %w", err)
	}
	var parsed []record
	if err := json.Unmarshal(text, &parsed); err != nil {
		return nil, fmt.Errorf("interpretar registros: %w", err)
	}
	groups := make(map[string]*groupTotals)
	for _, r := range parsed {
		totals, ok := groups[r.Group]
		if !ok {
			totals = &groupTotals{}
			groups[r.Group] = totals
		}
		totals.Count++
		totals.Sum += r.Value
	}
	// json.Marshal ordena as chaves do mapa, então a saída é estável.
	return json.Marshal(struct {
		Records    int                     `json:"records"`
		InputBytes int                     `json:"input_bytes"`
		Groups     map[string]*groupTotals `json:"groups"`
	}{Records: len(parsed), InputBytes: len(text), Groups: groups})
}
