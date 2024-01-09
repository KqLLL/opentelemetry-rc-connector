package opentelemetry_rc_connector

const (
	dbService    string = "db"
	redisService string = "redis"

	datadogDbSpanType    string = "DB"
	datadogCacheSpanType string = "Cache"
	datadogWebSpanType   string = "Web"
)

type Config struct{}
