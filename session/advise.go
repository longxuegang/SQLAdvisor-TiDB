package session

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type IndexAdviser struct{}

func (a *IndexAdviser) GetAdvise(stmt ast.StmtNode) {

}

type ResultSetAdvise struct {
	stmt *ast.AdviseStmt
	cnk *chunk.Chunk
	i int
}

func (r *ResultSetAdvise) Fields() []*ast.ResultField {
	log.Info("111111111")
	return []*ast.ResultField{{
		Table:       new(model.TableInfo),
		TableAsName: model.CIStr{},
		DBName:      model.CIStr{},
		Column: &model.ColumnInfo{
			ID:        0,
			Name:      model.NewCIStr("Hello"),
			FieldType: *types.NewFieldType(mysql.TypeString),
		},
		ColumnAsName:model.NewCIStr("Hello"),
	},
	}
}

func (r *ResultSetAdvise) Next(ctx context.Context, req *chunk.Chunk) error {
	log.Info("2222222")
	r.i++
	if r.i >=5 {
		req.Reset()
		return nil
	}
	req.NumRows()
	req.AppendString(0,"hello world")
	return  nil
}

func (r *ResultSetAdvise) NewChunk() *chunk.Chunk {
	log.Info("3333333")
	r.cnk= chunk.New([]*types.FieldType{types.NewFieldType(mysql.TypeString)},1,1)
return r.cnk
}

func (r *ResultSetAdvise) Close() error {
	log.Info("444444")
	return nil
}
