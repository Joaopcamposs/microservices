// Package domain reúne as regras puras do gateway: vocabulário do contrato, modelos e
// validação de payload. Não importa banco nem HTTP.
package domain

// JobType é o tipo de job do contrato (contracts/envelope.schema.json). Só os que têm schema
// em contracts/jobs são aceitos de fato.
type JobType string

// Tipos de job do contrato.
const (
	JobIOSleep           JobType = "io.sleep"
	JobIOFetchURLs       JobType = "io.fetch_urls"
	JobCPUPBKDF2         JobType = "cpu.pbkdf2"
	JobDataJSONTransform JobType = "data.json_transform"
	JobPipelineFanout    JobType = "pipeline.fanout"
)

// AllJobTypes lista os tipos do contrato; o validador usa para carregar os schemas existentes.
var AllJobTypes = []JobType{
	JobIOSleep, JobIOFetchURLs, JobCPUPBKDF2, JobDataJSONTransform, JobPipelineFanout,
}

// Valid informa se o valor é um tipo conhecido do contrato.
func (t JobType) Valid() bool {
	for _, known := range AllJobTypes {
		if t == known {
			return true
		}
	}
	return false
}

// Target é a stack que deve processar o job: "all" usa o exchange fanout, as demais o direct.
type Target string

// Alvos possíveis, iguais às constraints de db/migrations/001_init.sql.
const (
	TargetAll     Target = "all"
	TargetCelery  Target = "celery"
	TargetTaskiq  Target = "taskiq"
	TargetAsyncio Target = "asyncio"
	TargetGo      Target = "go"
)

// Valid informa se o valor é um alvo conhecido.
func (t Target) Valid() bool {
	switch t {
	case TargetAll, TargetCelery, TargetTaskiq, TargetAsyncio, TargetGo:
		return true
	}
	return false
}

// RoutingKey devolve a routing key da outbox: vazia para fanout, o nome da stack para direct.
func (t Target) RoutingKey() string {
	if t == TargetAll {
		return ""
	}
	return string(t)
}

// Origin identifica o gateway que recebeu o pedido; permite comparar os dois no benchmark.
type Origin string

// OriginGatewayGo é a origem deste serviço no envelope.
const OriginGatewayGo Origin = "gateway-go"

// OutboxState é o filtro de consulta da outbox.
type OutboxState string

// Estados de consulta da outbox.
const (
	OutboxPending   OutboxState = "pending"
	OutboxPublished OutboxState = "published"
	OutboxAll       OutboxState = "all"
)

// Valid informa se o valor é um filtro conhecido.
func (s OutboxState) Valid() bool {
	return s == OutboxPending || s == OutboxPublished || s == OutboxAll
}

// JobStatus é o estado agregado de um job, derivado da quantidade de resultados recebidos.
type JobStatus string

// Estados agregados de um job.
const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
)

// workersPerFanout é quantos resultados um job com target "all" espera (uma stack cada).
const workersPerFanout = 4
