// bulk_load_akumuli loads an akumlid daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/spf13/viper"
	"github.com/spf13/pflag"
	"github.com/bodhiye/tsbs/load"
	"github.com/bodhiye/tsbs/pkg/targets"
	"github.com/bodhiye/tsbs/pkg/targets/akumuli"
	"github.com/bodhiye/tsbs/pkg/targets/constants"
	"github.com/bodhiye/tsbs/pkg/targets/initializers"
	"github.com/bodhiye/tsbs/tools/utils"
)

// Program option vars:
var (
	endpoint string
)

// Global vars
var (
	loader     load.BenchmarkRunner
	loaderConf *load.BenchmarkRunnerConfig
)

// allows for testing
var fatal = log.Fatalf
var target targets.ImplementedTarget

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatAkumuli)
	loaderConf = &load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)

	pflag.Parse()
	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	endpoint = viper.GetString("endpoint")
	loaderConf.HashWorkers = true
	loader = load.GetBenchmarkRunner(*loaderConf)
}

func main() {
	bufPool := sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	benchmark := akumuli.NewBenchmark(loaderConf.FileName, endpoint, &bufPool)
	loader.RunBenchmark(benchmark)
}
