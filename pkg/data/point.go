package data

// Point wraps a single data point. It stores database-agnostic data
import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

// Point wraps a single data point. It stores database-agnostic data
// representing one point in time of one measurement.
//
// Internally, Point uses byte slices instead of strings to try to minimize
// overhead.
type Point struct {
	measurementName []byte
	tagKeys         [][]byte
	tagValues       []interface{}
	fieldKeys       [][]byte
	fieldValues     []interface{}
	timestamp       *time.Time
}

// NewPoint returns a new empty Point
func NewPoint() *Point {
	return &Point{
		measurementName: nil,
		tagKeys:         make([][]byte, 0),
		tagValues:       make([]interface{}, 0),
		fieldKeys:       make([][]byte, 0),
		fieldValues:     make([]interface{}, 0),
		timestamp:       nil,
	}
}

// Copy duplicates all the values from a given Point.
func (p *Point) Copy(from *Point) {
	p.measurementName = from.measurementName
	p.tagKeys = from.tagKeys
	p.tagValues = from.tagValues
	p.fieldKeys = from.fieldKeys
	p.fieldValues = from.fieldValues
	timeCopy := *from.timestamp
	p.timestamp = &timeCopy
}

// Reset clears all information from this Point so it can be reused.
func (p *Point) Reset() {
	p.measurementName = nil
	p.tagKeys = p.tagKeys[:0]
	p.tagValues = p.tagValues[:0]
	p.fieldKeys = p.fieldKeys[:0]
	p.fieldValues = p.fieldValues[:0]
	p.timestamp = nil
}

// SetTimestamp sets the Timestamp for this data point
func (p *Point) SetTimestamp(t *time.Time) {
	p.timestamp = t
}

func (p *Point) Timestamp() *time.Time {
	return p.timestamp
}

func (p *Point) TimestampInUnixMs() int64 {
	return p.timestamp.UnixNano() / 1000000
}

// SetMeasurementName sets the name of the measurement for this data point
func (p *Point) SetMeasurementName(s []byte) {
	p.measurementName = s
}

// MeasurementName returns the name of the Point's measurement
func (p *Point) MeasurementName() []byte {
	return p.measurementName
}

// FieldKeys returns the Point's field keys
func (p *Point) FieldKeys() [][]byte {
	return p.fieldKeys
}

// AppendField adds a field with a given key and value to this data point
func (p *Point) AppendField(key []byte, value interface{}) {
	p.fieldKeys = append(p.fieldKeys, key)
	p.fieldValues = append(p.fieldValues, value)
}

// GetFieldValue returns the corresponding value for a given field key or nil if it does not exist.
// This will panic if the internal state has been altered to not have the same number of field keys as field values.
func (p *Point) GetFieldValue(key []byte) interface{} {
	if len(p.fieldKeys) != len(p.fieldValues) {
		panic("field keys and field values are out of sync")
	}
	for i, v := range p.fieldKeys {
		if bytes.Equal(v, key) {
			return p.fieldValues[i]
		}
	}
	return nil
}

func (p *Point) FieldValues() []interface{} {
	return p.fieldValues
}

// ClearFieldValue sets the field value to nil for a given field key.
// This will panic if the internal state has been altered to not have the same number of field keys as field values.
func (p *Point) ClearFieldValue(key []byte) {
	if len(p.fieldKeys) != len(p.fieldValues) {
		panic("field keys and field values are out of sync")
	}
	for i, v := range p.fieldKeys {
		if bytes.Equal(v, key) {
			p.fieldValues[i] = nil
			return
		}
	}
}

// TagKeys returns the Point's tag keys
func (p *Point) TagKeys() [][]byte {
	return p.tagKeys
}

// AppendTag adds a tag with a given key and value to this data point
func (p *Point) AppendTag(key []byte, value interface{}) {
	p.tagKeys = append(p.tagKeys, key)
	p.tagValues = append(p.tagValues, value)
}

// GetTagValue returns the corresponding value for a given tag key or nil if it does not exist.
// This will panic if the internal state has been altered to not have the same number of tag keys as tag values.
func (p *Point) GetTagValue(key []byte) interface{} {
	if len(p.tagKeys) != len(p.tagValues) {
		panic("tag keys and tag values are out of sync")
	}
	for i, v := range p.tagKeys {
		if bytes.Equal(v, key) {
			return p.tagValues[i]
		}
	}
	return nil
}

func (p *Point) TagValues() []interface{} {
	return p.tagValues
}

// ClearTagValue sets the tag value to nil for a given field key.
// This will panic if the internal state has been altered to not have the same number of tag keys as tag values.
func (p *Point) ClearTagValue(key []byte) {
	if len(p.tagKeys) != len(p.tagValues) {
		panic("tag keys and tag values are out of sync")
	}
	for i, v := range p.tagKeys {
		if bytes.Equal(v, key) {
			p.tagValues[i] = nil
			return
		}
	}
}

// Point acts as a 'holder' for the internal representation of a point in a given load client.
// Instead of using interface{} as a return type, we get compile safety by using Point
type LoadedPoint struct {
	Data interface{}
}

// NewPoint creates a Point with the provided data as the internal representation
func NewLoadedPoint(data interface{}) LoadedPoint {
	return LoadedPoint{Data: data}
}

func (p *Point) DeepCopy() *Point {
	var newPoint = &Point{}
	newPoint.Copy(p)
	return newPoint
}

func (p *Point) String() string {
	buf := make([]byte, 0, 1024)
	buf = append(buf, p.MeasurementName()...)

	fakeTags := make([]int, 0)
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i := 0; i < len(tagKeys); i++ {
		if tagValues[i] == nil {
			continue
		}
		switch v := tagValues[i].(type) {
		case string:
			buf = append(buf, ',')
			buf = append(buf, tagKeys[i]...)
			buf = append(buf, '=')
			buf = append(buf, []byte(v)...)
		default:
			fakeTags = append(fakeTags, i)
		}
	}
	fieldKeys := p.FieldKeys()
	if len(fakeTags) > 0 || len(fieldKeys) > 0 {
		buf = append(buf, ' ')
	}
	firstFieldFormatted := false
	for i := 0; i < len(fakeTags); i++ {
		tagIndex := fakeTags[i]
		if firstFieldFormatted {
			buf = append(buf, ',')
		}
		firstFieldFormatted = true
		buf = appendField(buf, tagKeys[tagIndex], tagValues[tagIndex])
	}

	fieldValues := p.FieldValues()
	for i := 0; i < len(fieldKeys); i++ {
		value := fieldValues[i]
		if value == nil {
			continue
		}
		if firstFieldFormatted {
			buf = append(buf, ',')
		}
		firstFieldFormatted = true
		buf = appendField(buf, fieldKeys[i], value)
	}

	if !firstFieldFormatted {
		return ""
	}
	buf = append(buf, ' ')
	buf = fastFormatAppend(p.Timestamp().UTC().UnixNano(), buf)
	buf = append(buf, '\n')
	return string(buf)
}

func appendField(buf, key []byte, v interface{}) []byte {
	buf = append(buf, key...)
	buf = append(buf, '=')
	buf = fastFormatAppend(v, buf)
	switch v.(type) {
	case int, int64:
		buf = append(buf, 'i')
	}
	return buf
}

func fastFormatAppend(v interface{}, buf []byte) []byte {
	switch v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v.(int)), 10)
	case int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case float64:
		return strconv.AppendFloat(buf, v.(float64), 'f', -1, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', -1, 32)
	case bool:
		return strconv.AppendBool(buf, v.(bool))
	case []byte:
		buf = append(buf, v.([]byte)...)
		return buf
	case string:
		buf = append(buf, v.(string)...)
		return buf
	case nil:
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
