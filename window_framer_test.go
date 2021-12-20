package window_frames_toy

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"testing"
)

func TestWindowAggFramer(t *testing.T) {
	win := &window{
		blocks: []windowBlock{
			*newWindowBlock(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, sql.Int64, "foo", "c", false),
				},
				sql.SortFields{
					{
						Column: expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					},
				},
				[]windowAgg{
					newWindowSumAgg(expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
				},
				[]windowFramer{
					newRowsWindowFramer(2,0),
				},
				),
		},
	}

	inputRows := []sql.Row{
		sql.NewRow(int64(1), "first row", int32(42)),
		sql.NewRow(int64(2), "second row", int32(42)),
		sql.NewRow(int64(3), "third row", int32(42)),
		sql.NewRow(int64(1), "first row", int32(41)),
		sql.NewRow(int64(2), "second row", int32(41)),
		sql.NewRow(int64(3), "third row", int32(41)),
	}

	outputBuffer := make([]sql.Row, 1)

	// add input/output buffers

	// test execution
}