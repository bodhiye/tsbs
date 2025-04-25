package initializers

import (
	"fmt"
	"strings"

	"github.com/bodhiye/tsbs/pkg/targets"
	"github.com/bodhiye/tsbs/pkg/targets/akumuli"
	"github.com/bodhiye/tsbs/pkg/targets/cassandra"
	"github.com/bodhiye/tsbs/pkg/targets/clickhouse"
	"github.com/bodhiye/tsbs/pkg/targets/constants"
	"github.com/bodhiye/tsbs/pkg/targets/crate"
	"github.com/bodhiye/tsbs/pkg/targets/influx"
	"github.com/bodhiye/tsbs/pkg/targets/mongo"
	"github.com/bodhiye/tsbs/pkg/targets/prometheus"
	"github.com/bodhiye/tsbs/pkg/targets/questdb"
	"github.com/bodhiye/tsbs/pkg/targets/siridb"
	"github.com/bodhiye/tsbs/pkg/targets/timescaledb"
	"github.com/bodhiye/tsbs/pkg/targets/timestream"
	"github.com/bodhiye/tsbs/pkg/targets/victoriametrics"
)

func GetTarget(format string) targets.ImplementedTarget {
	switch format {
	case constants.FormatTimescaleDB:
		return timescaledb.NewTarget()
	case constants.FormatAkumuli:
		return akumuli.NewTarget()
	case constants.FormatCassandra:
		return cassandra.NewTarget()
	case constants.FormatClickhouse:
		return clickhouse.NewTarget()
	case constants.FormatCrateDB:
		return crate.NewTarget()
	case constants.FormatInflux:
		return influx.NewTarget()
	case constants.FormatMongo:
		return mongo.NewTarget()
	case constants.FormatPrometheus:
		return prometheus.NewTarget()
	case constants.FormatSiriDB:
		return siridb.NewTarget()
	case constants.FormatVictoriaMetrics:
		return victoriametrics.NewTarget()
	case constants.FormatTimestream:
		return timestream.NewTarget()
	case constants.FormatQuestDB:
		return questdb.NewTarget()
	}

	supportedFormatsStr := strings.Join(constants.SupportedFormats(), ",")
	panic(fmt.Sprintf("Unrecognized format %s, supported: %s", format, supportedFormatsStr))
}
