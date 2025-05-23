package clickhouse

import (
	"github.com/spf13/viper"
	"github.com/bodhiye/tsbs/pkg/data/serialize"
	"github.com/bodhiye/tsbs/pkg/data/source"
	"github.com/bodhiye/tsbs/pkg/targets"
	"github.com/bodhiye/tsbs/pkg/targets/constants"
	"github.com/bodhiye/tsbs/pkg/targets/timescaledb"
	"github.com/spf13/pflag"
)

func NewTarget() targets.ImplementedTarget {
	return &clickhouseTarget{}
}

type clickhouseTarget struct{}

func (c clickhouseTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("implement me")
}

func (c clickhouseTarget) Serializer() serialize.PointSerializer {
	return &timescaledb.Serializer{}
}

func (c clickhouseTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "localhost", "Hostname of ClickHouse instance")
	flagSet.String(flagPrefix+"user", "default", "User to connect to ClickHouse as")
	flagSet.String(flagPrefix+"password", "", "Password to connect to ClickHouse")
	flagSet.Bool(flagPrefix+"log-batches", false, "Whether to time individual batches.")
	flagSet.Int(flagPrefix+"debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
}

func (c clickhouseTarget) TargetName() string {
	return constants.FormatClickhouse
}
