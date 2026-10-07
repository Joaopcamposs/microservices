package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"microservices-lab/gateway-go/internal/domain"
	"microservices-lab/gateway-go/internal/service"
)

// maxBodyBytes limita o corpo do POST: payloads dos jobs são pequenos, e o limite evita que
// um cliente faça o gateway ler um corpo gigante.
const maxBodyBytes = 1 << 20

// Handler agrupa as rotas e o serviço que elas usam (injetado, sem estado global).
type Handler struct {
	jobs *service.JobService
}

// NewHandler recebe o JobService já montado.
func NewHandler(jobs *service.JobService) *Handler {
	return &Handler{jobs: jobs}
}

// fail responde no formato {"detail": "..."} usado pelos dois gateways.
func fail(c *gin.Context, status int, detail string) {
	c.JSON(status, ErrorBody{Detail: detail})
}

// SubmitJob recebe o pedido, delega ao serviço e responde 202 com o job_id.
//
//	@Summary		Enfileira um job
//	@Description	Valida o payload contra `contracts/jobs/<type>.schema.json` e grava o job e a outbox na mesma transação. Quem publica no RabbitMQ é o `outbox-relay`. `target=all` usa fanout (4 workers); os demais, um worker só.
//	@Tags			jobs
//	@Accept			json
//	@Produce		json
//	@Param			type	query		string		true	"Tipo do job."	Enums(io.sleep, io.fetch_urls, cpu.pbkdf2, data.json_transform, pipeline.fanout)	default(io.sleep)
//	@Param			target	query		string		false	"Stack que processa o job."	Enums(all, celery, taskiq, asyncio, go)	default(all)
//	@Param			payload	body		JobPayload	true	"Payload do tipo escolhido (hoje só io.sleep tem schema)."
//	@Success		202		{object}	JobAccepted
//	@Failure		422		{object}	ErrorBody
//	@Router			/jobs [post]
func (h *Handler) SubmitJob(c *gin.Context) {
	jobType := domain.JobType(c.Query("type"))
	if !jobType.Valid() {
		fail(c, http.StatusUnprocessableEntity, "type inválido ou ausente")
		return
	}
	target := domain.TargetAll
	if raw := c.Query("target"); raw != "" {
		target = domain.Target(raw)
	}
	if !target.Valid() {
		fail(c, http.StatusUnprocessableEntity, "target inválido")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes))
	if err != nil {
		fail(c, http.StatusUnprocessableEntity, "corpo ilegível ou grande demais")
		return
	}
	jobID, err := h.jobs.Submit(c.Request.Context(), jobType, target, body)
	switch {
	case errors.Is(err, domain.ErrInvalidPayload), errors.Is(err, domain.ErrUnsupportedJobType):
		fail(c, http.StatusUnprocessableEntity, err.Error())
	case err != nil:
		_ = c.Error(err)
		fail(c, http.StatusInternalServerError, "erro interno")
	default:
		c.JSON(http.StatusAccepted, JobAccepted{JobID: jobID})
	}
}

// pageQuery é o limite compartilhado pelas listagens.
type pageQuery struct {
	Limit int `form:"limit"`
}

// parseLimit lê `limit` (1..200) ou usa o padrão; devolve false se o valor é inválido.
func parseLimit(c *gin.Context, fallback int) (int, bool) {
	query := pageQuery{Limit: fallback}
	if err := c.ShouldBindQuery(&query); err != nil || query.Limit < 1 || query.Limit > 200 {
		fail(c, http.StatusUnprocessableEntity, "limit deve estar entre 1 e 200")
		return 0, false
	}
	return query.Limit, true
}

// ListJobs lista os jobs mais recentes.
//
//	@Summary		Lista os jobs mais recentes
//	@Description	Atalho para achar `job_id`s e ver o status de cada job sem abrir o banco.
//	@Tags			jobs
//	@Produce		json
//	@Param			limit	query		int	false	"Máximo de jobs."	minimum(1)	maximum(200)	default(20)
//	@Success		200		{array}		JobOut
//	@Failure		422		{object}	ErrorBody
//	@Router			/jobs [get]
func (h *Handler) ListJobs(c *gin.Context) {
	limit, ok := parseLimit(c, 20)
	if !ok {
		return
	}
	views, err := h.jobs.ListRecent(c.Request.Context(), limit)
	if err != nil {
		_ = c.Error(err)
		fail(c, http.StatusInternalServerError, "erro interno")
		return
	}
	out := make([]JobOut, 0, len(views))
	for _, view := range views {
		out = append(out, toJobOut(view))
	}
	c.JSON(http.StatusOK, out)
}

// GetJob devolve o job ou 404 se o job_id não existe.
//
//	@Summary		Consulta um job
//	@Description	Status agregado e resultado de cada worker.
//	@Tags			jobs
//	@Produce		json
//	@Param			job_id	path		string	true	"Identificador devolvido pelo POST /jobs."
//	@Success		200		{object}	JobOut
//	@Failure		404		{object}	ErrorBody
//	@Failure		422		{object}	ErrorBody
//	@Router			/jobs/{job_id} [get]
func (h *Handler) GetJob(c *gin.Context) {
	jobID := c.Param("job_id")
	if _, err := uuid.Parse(jobID); err != nil {
		fail(c, http.StatusUnprocessableEntity, "job_id não é um UUID")
		return
	}
	view, err := h.jobs.Get(c.Request.Context(), jobID)
	switch {
	case err != nil:
		_ = c.Error(err)
		fail(c, http.StatusInternalServerError, "erro interno")
	case view == nil:
		fail(c, http.StatusNotFound, "job não encontrado")
	default:
		c.JSON(http.StatusOK, toJobOut(*view))
	}
}

// ListOutbox lista linhas da outbox no estado pedido (padrão: aguardando o relay).
//
//	@Summary		Inspeciona a outbox
//	@Description	Linhas aguardando o relay (`pending`) ou já publicadas (`published`). Útil para conferir se o job saiu do gateway e se o relay está esvaziando a fila.
//	@Tags			debug
//	@Produce		json
//	@Param			state	query		string	false	"Filtro por estado."	Enums(pending, published, all)	default(pending)
//	@Param			limit	query		int		false	"Máximo de linhas."		minimum(1)	maximum(200)	default(50)
//	@Success		200		{array}		OutboxEntryOut
//	@Failure		422		{object}	ErrorBody
//	@Router			/outbox [get]
func (h *Handler) ListOutbox(c *gin.Context) {
	state := domain.OutboxPending
	if raw := c.Query("state"); raw != "" {
		state = domain.OutboxState(raw)
	}
	if !state.Valid() {
		fail(c, http.StatusUnprocessableEntity, "state deve ser pending, published ou all")
		return
	}
	limit, ok := parseLimit(c, 50)
	if !ok {
		return
	}
	entries, err := h.jobs.ListOutbox(c.Request.Context(), state, limit)
	if err != nil {
		_ = c.Error(err)
		fail(c, http.StatusInternalServerError, "erro interno")
		return
	}
	c.JSON(http.StatusOK, toOutboxOut(entries))
}

// Healthz responde 200 se o Postgres atende e 503 caso contrário.
//
//	@Summary	Liveness e conexão com o Postgres
//	@Tags		ops
//	@Produce	json
//	@Success	200	{object}	StatusBody
//	@Failure	503	{object}	ErrorBody
//	@Router		/healthz [get]
func (h *Handler) Healthz(c *gin.Context) {
	if err := h.jobs.CheckHealth(c.Request.Context()); err != nil {
		fail(c, http.StatusServiceUnavailable, "banco indisponível")
		return
	}
	c.JSON(http.StatusOK, StatusBody{Status: "ok"})
}
