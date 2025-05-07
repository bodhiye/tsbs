// tsbs_generate_queries generates queries for various use cases. Its output will
// be consumed by the corresponding tsbs_run_queries_ program.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bodhiye/tsbs/pkg/query/config"
	"github.com/bodhiye/tsbs/tools/inputs"
	"github.com/bodhiye/tsbs/tools/utils"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	conf          = &config.QueryGeneratorConfig{}
	useCaseMatrix = inputs.UseCaseMatrix
)

// Parse args:
func init() {
	useCaseMatrix["cpu-only"] = useCaseMatrix["devops"]
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := pflag.Usage
	pflag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt := range queryTypes {
				fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s\n", uc, qt)
			}
		}
	}

	conf.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&conf.BaseConfig); err != nil {
		panic(fmt.Errorf("unable to decode base config: %s", err))
	}

	if err := viper.Unmarshal(&conf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
}

func main() {
	qg := inputs.NewQueryGenerator(useCaseMatrix)
	queries, err := qg.Generate(conf)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
	log.Printf("Data-Generator generated %d queries\n", len(queries))
}
