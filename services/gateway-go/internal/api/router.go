package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// NewRouter monta o engine do Gin com as rotas, o Swagger e as métricas.
//
// Ser uma função (e não um global) deixa os testes criarem engines isolados. Gin.New + Recovery
// em vez de Default: o log por requisição fica fora para não interferir no benchmark.
func NewRouter(handler *Handler) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())

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
