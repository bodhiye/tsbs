package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bodhiye/tsbs/pkg/query"
	"github.com/bodhiye/tsbs/tools/utils"
)

// HLQuery is a high-level query, usually read from stdin after being
// generated by a bulk query generator program.
//
// The primary use of an HLQuery is to combine it with a ClientSideIndex to
// construct a QueryPlan.
type HLQuery struct {
	query.Cassandra
}

// String produces a debug-ready description of a Query.
func (q *HLQuery) String() string {
	return q.Cassandra.String()
}

// ForceUTC rewrites timestamps in UTC, which is helpful for pretty-printing.
func (q *HLQuery) ForceUTC() {
	q.TimeStart = q.TimeStart.UTC()
	q.TimeEnd = q.TimeEnd.UTC()
}

// ToQueryPlanWithServerAggregation combines an HLQuery with a
// ClientSideIndex to make a QueryPlanWithServerAggregation.
func (q *HLQuery) ToQueryPlanWithServerAggregation(csi *ClientSideIndex) (qp *QueryPlanWithServerAggregation, err error) {
	seriesChoices := csi.SeriesForMeasurementAndField(string(q.MeasurementName), string(q.FieldName))

	// Build the time buckets used for 'group by time'-type queries.
	//
	// It is important to populate these even if they end up being empty,
	// so that we get correct results for empty 'time buckets'.
	tis := bucketTimeIntervals(q.TimeStart, q.TimeEnd, q.GroupByDuration)
	bucketedSeries := map[*utils.TimeInterval][]Series{}
	for _, ti := range tis {
		bucketedSeries[ti] = []Series{}
	}

	// For each known db series, associate it to its applicable time
	// buckets, if any:
	for _, s := range seriesChoices {
		// quick skip if the series doesn't match at all:
		if !s.MatchesMeasurementName(string(q.MeasurementName)) {
			continue
		}
		if !s.MatchesFieldName(string(q.FieldName)) {
			continue
		}
		if !s.MatchesTagSets(q.TagSets) {
			continue
		}

		// check each group-by interval to see if it applies:
		for _, ti := range tis {
			if !s.MatchesTimeInterval(ti) {
				continue
			}
			bucketedSeries[ti] = append(bucketedSeries[ti], s)
		}
	}

	// For each group-by time bucket, convert its series into CQLQueries:
	cqlBuckets := make(map[*utils.TimeInterval][]CQLQuery, len(bucketedSeries))
	for ti, seriesSlice := range bucketedSeries {
		cqlQueries := make([]CQLQuery, len(seriesSlice))
		for i, ser := range seriesSlice {
			start := ti.Start()
			end := ti.End()

			// the following two special cases ensure equivalency with rounded time boundaries as seen in influxdb:
			// https://docs.influxdata.com/influxdb/v0.13/query_language/data_exploration/#rounded-group-by-time-boundaries
			if start.Before(q.TimeStart) {
				start = q.TimeStart
			}
			if end.After(q.TimeEnd) {
				end = q.TimeEnd
			}

			cqlQueries[i] = NewCQLQuery(string(q.AggregationType), ser.Table, ser.Id, string(q.OrderBy), start.UnixNano(), end.UnixNano())
		}
		cqlBuckets[ti] = cqlQueries
	}

	qp, err = NewQueryPlanWithServerAggregation(string(q.AggregationType), cqlBuckets)
	return
}

func (csi *ClientSideIndex) getSeriesChoicesForFieldsAndMeasurement(fields []string, measurement string) []Series {
	seriesChoices := make([]Series, 0)
	for _, f := range fields {
		seriesChoices = append(seriesChoices, csi.SeriesForMeasurementAndField(measurement, f)...)
	}

	return seriesChoices
}

// ToQueryPlanWithoutServerAggregation combines an HLQuery with a
// ClientSideIndex to make a QueryPlanWithoutServerAggregation.
//
// It executes at most one CQLQuery per series.
func (q *HLQuery) ToQueryPlanWithoutServerAggregation(csi *ClientSideIndex) (qp *QueryPlanWithoutServerAggregation, err error) {
	hlQueryInterval, err := utils.NewTimeInterval(q.TimeStart, q.TimeEnd)
	if err != nil {
		return nil, err
	}
	fields := strings.Split(string(q.FieldName), ",")
	seriesChoices := csi.getSeriesChoicesForFieldsAndMeasurement(fields, string(q.MeasurementName))
	orderBy := string(q.OrderBy)

	// Build the time buckets used for 'group by time'-type queries.
	//
	// It is important to populate these even if they end up being empty,
	// so that we get correct results for empty 'time buckets'.
	timeBuckets := bucketTimeIntervals(q.TimeStart, q.TimeEnd, q.GroupByDuration)

	// TODO more generalized?
	// Sort time buckets in reverse order if time descending for more
	// efficient query planning
	if orderBy == "timestamp_ns DESC" {
		for i, j := 0, len(timeBuckets)-1; i < j; i, j = i+1, j-1 {
			timeBuckets[i], timeBuckets[j] = timeBuckets[j], timeBuckets[i]
		}
	}

	// For each known db series, use it for querying only if it matches
	// this HLQuery:
	applicableSeries := []Series{}

outer:
	for _, s := range seriesChoices {
		if !s.MatchesMeasurementName(string(q.MeasurementName)) {
			continue
		}

		// Supports multiple fields separated by commas
		fieldFound := false
		for _, f := range fields {
			if !s.MatchesFieldName(f) {
				continue
			}
			fieldFound = true
			break
		}
		if !fieldFound {
			continue outer
		}

		if !s.MatchesTagSets(q.TagSets) {
			continue
		}
		if !s.MatchesTimeInterval(hlQueryInterval) {
			continue
		}

		applicableSeries = append(applicableSeries, s)
	}

	// Build CQLQuery objects that will be used to fulfill this HLQuery:
	cqlQueries := []CQLQuery{}
	for _, ser := range applicableSeries {
		cqlQueries = append(cqlQueries, NewCQLQuery("", ser.Table, ser.Id, orderBy, q.TimeStart.UnixNano(), q.TimeEnd.UnixNano()))
	}

	qp, err = NewQueryPlanWithoutServerAggregation(string(q.AggregationType), q.GroupByDuration, fields, timeBuckets, q.Limit, cqlQueries)
	return
}

// ToQueryPlanNoAggregation combines an HLQuery with a
// ClientSideIndex to make a QueryPlanNoAggregation.
func (q *HLQuery) ToQueryPlanNoAggregation(csi *ClientSideIndex) (*QueryPlanNoAggregation, error) {
	hlQueryInterval, err := utils.NewTimeInterval(q.TimeStart, q.TimeEnd)
	if err != nil {
		return nil, err
	}
	fields := strings.Split(string(q.FieldName), ",")
	seriesChoices := csi.getSeriesChoicesForFieldsAndMeasurement(fields, string(q.MeasurementName))

	// For each known db series, use it for querying only if it matches
	// this HLQuery (its tagsets and time interval):
	applicableSeries := []Series{}
	for _, s := range seriesChoices {
		// If no tagsets given, return all that match time
		if len(q.TagSets) > 0 {
			if !s.MatchesTagSets(q.TagSets) {
				continue
			}
		}

		if !s.MatchesTimeInterval(hlQueryInterval) {
			continue
		}

		applicableSeries = append(applicableSeries, s)
	}

	// Build CQLQuery objects that will be used to fulfill this HLQuery:
	cqlQueries := []CQLQuery{}
	whereClause := string(q.WhereClause)
	for _, ser := range applicableSeries {
		cqlQueries = append(cqlQueries, NewCQLQuery("", ser.Table, ser.Id, string(q.OrderBy), q.TimeStart.UnixNano(), q.TimeEnd.UnixNano()))
	}

	return NewQueryPlanNoAggregation(fields, whereClause, cqlQueries)
}

// ToQueryPlanForEvery combines an HLQuery with a
// ClientSideIndex to make a QueryPlanForEvery.
func (q *HLQuery) ToQueryPlanForEvery(csi *ClientSideIndex) (*QueryPlanForEvery, error) {
	forEveryArgs := strings.Split(string(q.ForEveryN), ",")
	forEveryTag := forEveryArgs[0]
	forEveryNum, err := strconv.ParseInt(forEveryArgs[1], 10, 0)
	if err != nil {
		panic("unparseable ForEveryN field: " + string(q.ForEveryN))
	}

	hlQueryInterval, err := utils.NewTimeInterval(q.TimeStart, q.TimeEnd)
	if err != nil {
		return nil, err
	}
	fields := strings.Split(string(q.FieldName), ",")
	seriesChoices := csi.getSeriesChoicesForFieldsAndMeasurement(fields, string(q.MeasurementName))

	// For each known db series, use it for querying only if it matches
	// this HLQuery (its tagsets and time interval):
	applicableSeries := []Series{}
	for _, s := range seriesChoices {

		// If no tagsets given, return all that match time
		if len(q.TagSets) > 0 {
			if !s.MatchesTagSets(q.TagSets) {
				continue
			}
		}

		if !s.MatchesTimeInterval(hlQueryInterval) {
			continue
		}

		applicableSeries = append(applicableSeries, s)
	}

	// Build CQLQuery objects that will be used to fulfill this HLQuery:
	cqlQueries := []CQLQuery{}
	for _, ser := range applicableSeries {
		cqlQ := NewCQLQuery("", ser.Table, ser.Id, "timestamp_ns DESC", q.TimeStart.UnixNano(), q.TimeEnd.UnixNano())
		cqlQ.PreparableQueryString += " LIMIT 1"
		cqlQueries = append(cqlQueries, cqlQ)
	}

	return NewQueryPlanForEvery(fields, forEveryTag, forEveryNum, cqlQueries)
}

// CQLQuery wraps data needed to execute a gocql.Query.
type CQLQuery struct {
	PreparableQueryString string
	Args                  []interface{}
	Field                 string
}

// NewCQLQuery builds a CQLQuery, using prepared CQL statements.
func NewCQLQuery(aggrLabel, tableName, rowName, orderBy string, timeStartNanos, timeEndNanos int64) CQLQuery {
	var preparableQueryString string

	if len(aggrLabel) == 0 {
		orderByClause := ""
		if len(orderBy) > 0 {
			orderByClause = "ORDER BY " + orderBy
		}

		preparableQueryString = fmt.Sprintf("SELECT timestamp_ns, value FROM %s WHERE series_id = ? AND timestamp_ns >= ? AND timestamp_ns < ? %s", tableName, orderByClause)
	} else {
		preparableQueryString = fmt.Sprintf("SELECT %s(value) FROM %s WHERE series_id = ? AND timestamp_ns >= ? AND timestamp_ns < ?", aggrLabel, tableName)
	}
	args := []interface{}{rowName, timeStartNanos, timeEndNanos}
	rowParts := strings.Split(rowName, "#")
	return CQLQuery{preparableQueryString, args, rowParts[len(rowParts)-2]}
}

// CQLResult holds a result from a set of CQL aggregation queries.
// Used for debug printing.
type CQLResult struct {
	*utils.TimeInterval
	Values []float64
}
