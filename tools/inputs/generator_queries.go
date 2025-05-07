package inputs

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	queryUtils "github.com/bodhiye/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/bodhiye/tsbs/pkg/data/usecases/common"
	"github.com/bodhiye/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/bodhiye/tsbs/cmd/tsbs_generate_queries/uses/iot"

	"github.com/bodhiye/tsbs/pkg/query"
	"github.com/bodhiye/tsbs/pkg/query/config"
	"github.com/bodhiye/tsbs/pkg/query/factories"
	internalUtils "github.com/bodhiye/tsbs/tools/utils"
)

// Error messages when using a QueryGenerator
const (
	errBadQueryTypeFmt          = "invalid query type for use case '%s': '%s'"
	errCouldNotDebugFmt         = "could not write debug output: %v"
	errCouldNotEncodeQueryFmt   = "could not encode query: %v"
	errCouldNotQueryStatsFmt    = "could not output query stats: %v"
	errUseCaseNotImplementedFmt = "use case '%s' not implemented for format '%s'"
	errInvalidFactory           = "query generator factory for database '%s' does not implement the correct interface"
	errUnknownUseCaseFmt        = "use case '%s' is undefined"
	errCannotParseTimeFmt       = "cannot parse time from string '%s': %v"
	errBadUseFmt                = "invalid use case specified: '%v'"
)

var UseCaseMatrix = map[string]map[string]queryUtils.QueryFillerMaker{
	"devops": {
		devops.LabelSingleGroupby + "-1-1-1":  devops.NewSingleGroupby(1, 1, 1),
		devops.LabelSingleGroupby + "-1-1-12": devops.NewSingleGroupby(1, 1, 12),
		devops.LabelSingleGroupby + "-1-8-1":  devops.NewSingleGroupby(1, 8, 1),
		devops.LabelSingleGroupby + "-5-1-1":  devops.NewSingleGroupby(5, 1, 1),
		devops.LabelSingleGroupby + "-5-1-12": devops.NewSingleGroupby(5, 1, 12),
		devops.LabelSingleGroupby + "-5-8-1":  devops.NewSingleGroupby(5, 8, 1),
		devops.LabelMaxAll + "-1":             devops.NewMaxAllCPU(1, devops.MaxAllDuration),
		devops.LabelMaxAll + "-8":             devops.NewMaxAllCPU(8, devops.MaxAllDuration),
		devops.LabelMaxAll + "-32-24":         devops.NewMaxAllCPU(32, 24*time.Hour),
		devops.LabelDoubleGroupby + "-1":      devops.NewGroupBy(1),
		devops.LabelDoubleGroupby + "-5":      devops.NewGroupBy(5),
		devops.LabelDoubleGroupby + "-all":    devops.NewGroupBy(devops.GetCPUMetricsLen()),
		devops.LabelGroupbyOrderbyLimit:       devops.NewGroupByOrderByLimit,
		devops.LabelHighCPU + "-all":          devops.NewHighCPU(0),
		devops.LabelHighCPU + "-1":            devops.NewHighCPU(1),
		devops.LabelLastpoint:                 devops.NewLastPointPerHost,
	},
	"iot": {
		iot.LabelLastLoc:                       iot.NewLastLocPerTruck,
		iot.LabelLastLocSingleTruck:            iot.NewLastLocSingleTruck,
		iot.LabelLowFuel:                       iot.NewTruckWithLowFuel,
		iot.LabelHighLoad:                      iot.NewTruckWithHighLoad,
		iot.LabelStationaryTrucks:              iot.NewStationaryTrucks,
		iot.LabelLongDrivingSessions:           iot.NewTrucksWithLongDrivingSession,
		iot.LabelLongDailySessions:             iot.NewTruckWithLongDailySession,
		iot.LabelAvgVsProjectedFuelConsumption: iot.NewAvgVsProjectedFuelConsumption,
		iot.LabelAvgDailyDrivingDuration:       iot.NewAvgDailyDrivingDuration,
		iot.LabelAvgDailyDrivingSession:        iot.NewAvgDailyDrivingSession,
		iot.LabelAvgLoad:                       iot.NewAvgLoad,
		iot.LabelDailyActivity:                 iot.NewDailyTruckActivity,
		iot.LabelBreakdownFrequency:            iot.NewTruckBreakdownFrequency,
	},
}

// DevopsGeneratorMaker creates a query generator for devops use case
type DevopsGeneratorMaker interface {
	NewDevops(start, end time.Time, scale int) (queryUtils.QueryGenerator, error)
}

// IoTGeneratorMaker creates a quert generator for iot use case
type IoTGeneratorMaker interface {
	NewIoT(start, end time.Time, scale int) (queryUtils.QueryGenerator, error)
}

// QueryGenerator is a type of Generator for creating queries to test against a
// database. The output is specific to the type of database (due to each using
// different querying techniques, e.g. SQL or REST), but is consumed by TSBS
// query runners like tsbs_run_queries_timescaledb.
type QueryGenerator struct {
	// Out is the writer where data should be written. If nil, it will be
	// os.Stdout unless File is specified in the GeneratorConfig passed to
	// Generate.
	Out io.Writer
	// DebugOut is where non-generated messages should be written. If nil, it
	// will be os.Stderr.
	DebugOut io.Writer

	conf          *config.QueryGeneratorConfig
	useCaseMatrix map[string]map[string]queryUtils.QueryFillerMaker
	// factories contains all the database implementations which can create
	// devops query generators.
	factories map[string]interface{}
	tsStart   time.Time
	tsEnd     time.Time

	// bufOut represents the buffered writer that should actually be passed to
	// any operations that write out data.
	bufOut *bufio.Writer
}

// NewQueryGenerator returns a QueryGenerator that is set up to work with a given
// useCaseMatrix, which tells it how to generate the given query type for a use
// case.
func NewQueryGenerator(useCaseMatrix map[string]map[string]queryUtils.QueryFillerMaker) *QueryGenerator {
	return &QueryGenerator{
		useCaseMatrix: useCaseMatrix,
		factories:     make(map[string]interface{}),
	}
}

func (g *QueryGenerator) Generate(config common.GeneratorConfig) ([]string, error) {
	err := g.init(config)
	if err != nil {
		return nil, err
	}

	useGen, err := g.getUseCaseGenerator(g.conf)
	if err != nil {
		return nil, err
	}

	filler := g.useCaseMatrix[g.conf.Use][g.conf.QueryType](useGen)

	return g.runQueryGeneration(useGen, filler, g.conf)
}

func (g *QueryGenerator) init(conf common.GeneratorConfig) error {
	if conf == nil {
		return fmt.Errorf(ErrNoConfig)
	}
	switch conf.(type) {
	case *config.QueryGeneratorConfig:
	default:
		return fmt.Errorf(ErrInvalidDataConfig)
	}
	g.conf = conf.(*config.QueryGeneratorConfig)

	err := g.conf.Validate()
	if err != nil {
		return err
	}

	if err := g.initFactories(); err != nil {
		return err
	}

	if _, ok := g.useCaseMatrix[g.conf.Use]; !ok {
		return fmt.Errorf(errBadUseFmt, g.conf.Use)
	}

	if _, ok := g.useCaseMatrix[g.conf.Use][g.conf.QueryType]; !ok {
		return fmt.Errorf(errBadQueryTypeFmt, g.conf.Use, g.conf.QueryType)
	}

	g.tsStart, err = internalUtils.ParseUTCTime(g.conf.TimeStart)
	if err != nil {
		return fmt.Errorf(errCannotParseTimeFmt, g.conf.TimeStart, err)
	}
	g.tsEnd, err = internalUtils.ParseUTCTime(g.conf.TimeEnd)
	if err != nil {
		return fmt.Errorf(errCannotParseTimeFmt, g.conf.TimeEnd, err)
	}

	if g.Out == nil {
		g.Out = os.Stdout
	}
	g.bufOut, err = getBufferedWriter(g.conf.File, g.Out)
	if err != nil {
		return err
	}

	if g.DebugOut == nil {
		g.DebugOut = os.Stderr
	}

	return nil
}

func (g *QueryGenerator) initFactories() error {
	factoryMap := factories.InitQueryFactories(g.conf)
	for db, fac := range factoryMap {
		if err := g.addFactory(db, fac); err != nil {
			return err
		}
	}
	return nil
}

func (g *QueryGenerator) addFactory(database string, factory interface{}) error {
	validFactory := false

	switch factory.(type) {
	case DevopsGeneratorMaker, IoTGeneratorMaker:
		validFactory = true
	}

	if !validFactory {
		return fmt.Errorf(errInvalidFactory, database)
	}

	g.factories[database] = factory

	return nil
}

func (g *QueryGenerator) getUseCaseGenerator(c *config.QueryGeneratorConfig) (queryUtils.QueryGenerator, error) {
	scale := int(c.Scale) // TODO: make all the Devops constructors use a uint64
	var factory interface{}
	var ok bool

	if factory, ok = g.factories[c.Format]; !ok {
		return nil, fmt.Errorf(errUnknownFormatFmt, c.Format)
	}

	switch c.Use {
	case common.UseCaseIoT:
		iotFactory, ok := factory.(IoTGeneratorMaker)

		if !ok {
			return nil, fmt.Errorf(errUseCaseNotImplementedFmt, c.Use, c.Format)
		}

		return iotFactory.NewIoT(g.tsStart, g.tsEnd, scale)
	case common.UseCaseDevops, common.UseCaseCPUOnly, common.UseCaseCPUSingle:
		devopsFactory, ok := factory.(DevopsGeneratorMaker)
		if !ok {
			return nil, fmt.Errorf(errUseCaseNotImplementedFmt, c.Use, c.Format)
		}

		return devopsFactory.NewDevops(g.tsStart, g.tsEnd, scale)
	default:
		return nil, fmt.Errorf(errUnknownUseCaseFmt, c.Use)
	}
}

func (g *QueryGenerator) runQueryGeneration(useGen queryUtils.QueryGenerator, filler queryUtils.QueryFiller, c *config.QueryGeneratorConfig) ([]string, error) {
	stats := make(map[string]int64)
	currentGroup := uint(0)
	enc := gob.NewEncoder(g.bufOut)
	defer g.bufOut.Flush()

	rand.Seed(g.conf.Seed)
	//fmt.Println(g.config.Seed)
	if g.conf.Debug > 0 {
		_, err := fmt.Fprintf(g.DebugOut, "using random seed %d\n", g.conf.Seed)
		if err != nil {
			return nil, fmt.Errorf(errCouldNotDebugFmt, err)
		}
	}

	var queries = make([]string, 0)
	for i := 0; i < int(c.Limit); i++ {
		q := useGen.GenerateEmptyQuery()
		q = filler.Fill(q)

		if currentGroup == c.InterleavedGroupID {
			err := enc.Encode(q)
			if err != nil {
				return nil, fmt.Errorf(errCouldNotEncodeQueryFmt, err)
			}
			stats[string(q.HumanLabelName())]++

			if c.Debug > 0 {
				var debugMsg string
				if c.Debug == 1 {
					debugMsg = string(q.HumanLabelName())
				} else if c.Debug == 2 {
					debugMsg = string(q.HumanDescriptionName())
				} else if c.Debug >= 3 {
					debugMsg = q.String()
				}

				_, err = fmt.Fprintf(g.DebugOut, debugMsg+"\n")
				if err != nil {
					return nil, fmt.Errorf(errCouldNotDebugFmt, err)
				}
			}
		}
		hq := q.(*query.HTTP)
		decodedPath, err := url.QueryUnescape(string(hq.Path))
		if err != nil {
			return nil, err
		}
		queries = append(queries, strings.TrimPrefix(decodedPath, "/query?q="))
		q.Release()

		currentGroup++
		if currentGroup == c.InterleavedNumGroups {
			currentGroup = 0
		}
	}

	// Print stats:
	keys := []string{}
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, err := fmt.Fprintf(g.DebugOut, "%s: %d points\n", k, stats[k])
		if err != nil {
			return nil, fmt.Errorf(errCouldNotQueryStatsFmt, err)
		}
	}
	return queries, nil
}
