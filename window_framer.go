package window_frames_toy

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"io"
	"sort"
)

type window struct {
	blocks []windowBlock
}

func (w *window) shortEval(input windowBuffer) {
	output := make(windowBuffer, 0)
	for _, block := range w.blocks {
		block.input = input
		block.output = output
		block.shortEval()
	}
}

// windowBlock is a set of aggregations for a specific
// partition key and sort key set (WPK, WSK). The block of
// windowAgg will share a windowBuffer.
type windowBlock struct {
	partitionBy []sql.Expression
	sortBy sql.SortFields
	unordered bool

	windowAggs []windowAgg
	framers []windowFramer

	input windowBuffer
	output windowBuffer
	outputIdx []int
	partitions []windowInterval
}

func newWindowBlock(partitionBy []sql.Expression, sortBy sql.SortFields, aggs []windowAgg, framers []windowFramer) *windowBlock {
	return &windowBlock{
		partitionBy: partitionBy,
		sortBy: sortBy,
		windowAggs: aggs,
		framers: framers,
	}
}

func (w *windowBlock) withInput(buf windowBuffer) {
	w.input = buf
}

func (w *windowBlock) withOutput(buf windowBuffer, outputIdx []int) {
	w.output = buf
}

func (w *windowBlock) shortEval() {
	// sort buffer
	w.initializeInputBuffer(sql.NewEmptyContext())

	// identify partitions

	// perform aggregations
	for _, part := range w.partitions {
		for _, agg := range w.windowAggs {
			agg.startPartition(part)
		}

		for _, framer := range w.framers {
			framer.startPartition(part)
		}

		for j := part.start; j < part.end; j++ {
			for k, agg := range w.windowAggs {
				interval, _ := w.framers[k].next()
				res := agg.compute(interval)
				outputIdx := w.outputIdx[k]
				w.output[outputIdx] = sql.NewRow(res)
			}
		}
	}
}

func partitionsToSortFields(partitionExprs []sql.Expression) sql.SortFields {
	sfs := make(sql.SortFields, len(partitionExprs))
	for i, expr := range partitionExprs {
		sfs[i] = sql.SortField{
			Column: expr,
			Order:  sql.Ascending,
		}
	}
	return sfs
}

// initializeInputBuffer sorts the buffer by (WPK, WSK)
// if the block is not unordered
func (w *windowBlock) initializeInputBuffer(ctx *sql.Context) {
	sorter := &expression.Sorter{
		SortFields: append(partitionsToSortFields(w.partitionBy), w.sortBy...),
		Rows:       w.input,
		Ctx:        ctx,
	}
	sort.Stable(sorter)
}

// Every windowAgg in a block reuses the same buffer.
// TODO: is the output buffer a separate struct?
type windowBuffer []sql.Row

// windowAgg tracks the internal state for partitions
// passed by windowBlock. implementations can
// use internal data structures to optimize execution.
// TODO: should windowBlock support data structure sharing?
type windowAgg interface {
	// startPartition resets the internal agg state
	startPartition(windowInterval)
	// certain windowAgg functions can perform linear updates between the current
	// and previous states
	newSlidingFrameInterval(added, dropped windowInterval)
	//
	compute(windowInterval) interface{}
}

var _ windowAgg = (*windowSumAgg)(nil)
var _ windowAgg = (*windowFirstAgg)(nil)

type windowSumAgg struct {
	buf windowBuffer
	partitionStart, partitionEnd int
	prevInterval *windowInterval
	expr sql.Expression
	prevSum int
}

func newWindowSumAgg(e sql.Expression) *windowSumAgg {
	return &windowSumAgg{
		partitionStart: -1,
		partitionEnd: -1,
		expr: e,
	}
}

func (s *windowSumAgg) startPartition(interval windowInterval) {
	s.partitionStart, s.partitionEnd = interval.start, interval.end
	s.prevSum = 0
	s.prevInterval = nil
}

func (s *windowSumAgg) newSlidingFrameInterval(added, dropped windowInterval) {
	return
}

func (s *windowSumAgg) compute(interval windowInterval) interface{} {
	var res int
	for i := interval.start; i < interval.end; i++ {
		val, _ := s.expr.Eval(sql.NewEmptyContext(), s.buf[i])
		res += val.(int)
	}
	return res
}

type windowFirstAgg struct {
	buf windowBuffer
	partitionStart, partitionEnd int
}

func (s *windowFirstAgg) startPartition(interval windowInterval) {
	s.partitionStart, s.partitionEnd = interval.start, interval.end
}

func (s *windowFirstAgg) newSlidingFrameInterval(added, dropped windowInterval) {
	return
}

func (s *windowFirstAgg) compute(interval windowInterval) interface{} {
	return s.partitionStart
}

// windowFramer is responsible for tracking window frame indices for partition rows.
// windowFramer is aware of the framing strategy (offsets, ranges, etc),
// and is responsible for returning a windowInterval for each partition row.
type windowFramer interface {
	// reset internal state
	startPartition(windowInterval)
	// process next row, recalculate frame
	next() (windowInterval, error)
	// interval start index
	frameFirstIdx()
	// interval end index
	frameLastIdx()
	// TODO there are circumstances where a slice return might make sense
	frameInterval() (windowInterval, error)
	// TODO sliding window optimization tracks the current interval, the new added interval, and the deleted interval
	slidingFrameInvterval(ctx sql.Context) (windowInterval, windowInterval, windowInterval)
	close()
}

var _ windowFramer = (*rowsWindowFramer)(nil)

type rowsWindowFramer struct {
	buf *windowBuffer
	idx int
	partitionStart, partitionEnd int

	followOffset, precOffset int
	frameStart, frameEnd int
	frameSet bool
}

func newRowsWindowFramer(followOffset, precOffset int) *rowsWindowFramer {
	return &rowsWindowFramer{
		followOffset: followOffset,
		precOffset: precOffset,
		frameEnd: -1,
		frameStart: -1,
		partitionStart: -1,
		partitionEnd: -1,
	}
}

func (r *rowsWindowFramer) startPartition(part windowInterval) {
	r.idx = part.start
	r.partitionStart = part.start
	r.partitionEnd = part.end
	r.frameStart = -1
	r.frameEnd = -1
	r.frameSet = false
}

func (r *rowsWindowFramer) next() (windowInterval, error) {
	if r.idx > r.partitionEnd {
		return windowInterval{}, io.EOF
	}

	r.frameSet = false
	defer func() {r.frameSet = true}()

	if r.frameStart == -1 {
		r.frameStart = r.partitionStart
		r.frameEnd = r.partitionStart + r.followOffset
		return r.frameInterval()
	}

	newStart := r.idx - r.precOffset
	if newStart < 0 {
		newStart = 0
	}

	newEnd := r.idx + r.precOffset
	if newEnd > r.partitionEnd {
		newEnd = r.partitionEnd
	}

	r.frameStart = newStart
	r.frameEnd = newEnd

	return r.frameInterval()
}

func (r *rowsWindowFramer) frameFirstIdx() {
	panic("implement me")
}

func (r *rowsWindowFramer) frameLastIdx() {
	panic("implement me")
}

func (r *rowsWindowFramer) frameInterval() (windowInterval, error) {
	return windowInterval{start: r.frameStart, end: r.frameEnd}, nil
}

func (r *rowsWindowFramer) slidingFrameInvterval(ctx sql.Context) (windowInterval, windowInterval, windowInterval) {
	panic("implement me")
}

func (r *rowsWindowFramer) close() {
	panic("implement me")
}

// range for
type windowInterval struct {
    start, end int
}


func main() {

}
