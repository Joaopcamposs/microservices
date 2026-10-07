package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/trace"
)

// NewRouter monta o engine do Gin com as rotas, o Swagger e as métricas.
//
// Ser uma função (e não um global) deixa os testes criarem engines isolados. Gin.New + Recovery
// em vez de Default: o log por requisição fica fora para não interferir no benchmark.
func NewRouter(handler *Handler, tracerProvider trace.TracerProvider) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
	// Span HTTP raiz de cada requisição; o span "enqueue job" nasce como filho dele. Rotas de
	// operação (métricas, saúde, Swagger) ficam fora para não poluir o Jaeger.
	router.Use(otelgin.Middleware("gateway-go", otelgin.WithTracerProvider(tracerProvider),
		otelgin.WithFilter(func(r *http.Request) bool { return isTraced(r.URL.Path) })))

	router.POST("/jobs", handler.SubmitJob)
	router.GET("/jobs", handler.ListJobs)
	router.GET("/jobs/:job_id", handler.GetJob)
	router.GET("/outbox", handler.ListOutbox)
	router.GET("/healthz", handler.Healthz)

	// Swagger UI em /docs, o mesmo endereço do FastAPI (interface "Try it out").
	router.GET("/docs/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	// /docs sem barra e o antigo /swagger redirecionam para a UI.
	toUI := func(c *gin.Context) { c.Redirect(http.StatusFound, "/docs/index.html") }
	router.GET("/docs", toUI)
	router.GET("/swagger", toUI)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	return router
}

// isTraced diz se o caminho gera span: só as rotas de jobs e da outbox interessam ao trace.
func isTraced(path string) bool {
	return strings.HasPrefix(path, "/jobs") || path == "/outbox"
}
