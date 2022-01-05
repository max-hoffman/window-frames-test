package window_frames_toy

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWindowAggFramer(t *testing.T) {
	win := &window{
		blocks: []*windowBlock{
			newWindowBlock(
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
					newWindowSumAgg(expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false)),
				},
				[]windowFramer{
					newRowsWindowFramer(2,0),
					newRowsWindowFramer(1,1),
				},
				),
		},
	}

	inputRows := []sql.Row{
		sql.NewRow(1, "first row", int32(42)),
		sql.NewRow(2, "second row", int32(42)),
		sql.NewRow(3, "third row", int32(42)),
		sql.NewRow(1, "first row", int32(41)),
		sql.NewRow(2, "second row", int32(41)),
		sql.NewRow(3, "third row", int32(41)),
	}

	outputBuffer := make([]sql.Row, 2)

	// add input/output buffers
	win = win.withInput(inputRows).withOutput(outputBuffer)
	//win.withOutput(outputBuffer)
	// test execution
	win.shortEval()
	//fmt.Println(win.blocks[0].output)
	exp := windowBuffer{
		{6, 5, 3, 6, 5, 3},
		{3, 6, 5, 3, 6, 5},
	}
	assert.Equal(t, exp, win.blocks[0].output)
}