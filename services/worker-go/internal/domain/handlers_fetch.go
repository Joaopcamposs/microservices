package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// fetchTimeout é o timeout de cada GET do io.fetch_urls; igual nas quatro stacks.
const fetchTimeout = 10 * time.Second

// fetchResult resume a resposta de uma URL, no formato definido pelo contrato do job.
type fetchResult struct {
	URL    string `json:"url"`
	Status int    `json:"status"`
	Bytes  int64  `json:"bytes"`
}

// handleIOFetchURLs busca todas as URLs ao mesmo tempo (uma goroutine por URL) e devolve os
// resumos na ordem recebida. Sem redirects, para se comportar como as outras stacks; qualquer
// falha de rede derruba o job inteiro, pois um resultado parcial não seria comparável.
func handleIOFetchURLs(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var input struct {
		URLs []string `json:"urls"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("payload io.fetch_urls: %w", err)
	}
	client := &http.Client{
		Timeout: fetchTimeout,
		// Devolve a resposta 3xx em vez de seguir o redirect.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	results := make([]fetchResult, len(input.URLs))
	errs := make([]error, len(input.URLs))
	var wg sync.WaitGroup
	for i, url := range input.URLs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = fetchOne(ctx, client, url)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(struct {
		Results []fetchResult `json:"results"`
	}{Results: results})
}

// fetchOne faz um GET e lê o corpo inteiro para contar os bytes.
func fetchOne(ctx context.Context, client *http.Client, url string) (fetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fetchResult{}, fmt.Errorf("montar requisição %s: %w", url, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fetchResult{}, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	size, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return fetchResult{}, fmt.Errorf("ler corpo de %s: %w", url, err)
	}
	return fetchResult{URL: url, Status: resp.StatusCode, Bytes: size}, nil
}
