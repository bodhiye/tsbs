package iot

import (
	"github.com/bodhiye/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/bodhiye/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/bodhiye/tsbs/pkg/query"
)

// AvgLoad contains info for filling in avg load queries.
type AvgLoad struct {
	core utils.QueryGenerator
}

// NewAvgLoad creates a new avg load query filler.
func NewAvgLoad(core utils.QueryGenerator) utils.QueryFiller {
	return &AvgLoad{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *AvgLoad) Fill(q query.Query) query.Query {
	fc, ok := i.core.(AvgLoadFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.AvgLoad(q)
	return q
}
