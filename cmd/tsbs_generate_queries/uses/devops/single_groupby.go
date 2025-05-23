package devops

import (
	"time"

	"github.com/bodhiye/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/bodhiye/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/bodhiye/tsbs/pkg/query"
)

// SingleGroupby contains info for filling in single groupby queries
type SingleGroupby struct {
	core    utils.QueryGenerator
	metrics int
	hosts   int
	hours   int
}

// NewSingleGroupby produces a new function that produces a new SingleGroupby
func NewSingleGroupby(metrics, hosts, hours int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &SingleGroupby{
			core:    core,
			metrics: metrics,
			hosts:   hosts,
			hours:   hours,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *SingleGroupby) Fill(q query.Query) query.Query {
	fc, ok := d.core.(SingleGroupbyFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.GroupByTime(q, d.hosts, d.metrics, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
