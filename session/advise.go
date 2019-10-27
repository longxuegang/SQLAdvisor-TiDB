package session

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"go.uber.org/zap"
)

type IndexAdviser struct{}

func (a *IndexAdviser) GetAdvise(stmt ast.StmtNode) {

}

type ResultSetAdvise struct {
	stmt    *ast.AdviseStmt
	cnk     *chunk.Chunk
	session *session
	is      infoschema.InfoSchema
	i       int
}

func (r *ResultSetAdvise) Fields() []*ast.ResultField {
	log.Info("111111111")
	return []*ast.ResultField{{
		Table:       new(model.TableInfo),
		TableAsName: model.CIStr{},
		DBName:      model.CIStr{},
		Column: &model.ColumnInfo{
			ID:        0,
			Name:      model.NewCIStr("ADVISE_INDEX"),
			FieldType: *types.NewFieldType(mysql.TypeString),
		},
		ColumnAsName: model.NewCIStr("ADVISE_INDEX"),
	},
	}
}

func (r *ResultSetAdvise) Next(ctx context.Context, req *chunk.Chunk) error {
	log.Info("2222222")
	r.i++
	if r.i > 1 {
		req.Reset()
		return nil
	}
	currentDB := r.session.sessionVars.CurrentDB
	strs, err := advise(r, currentDB, r.stmt.SelectStmt)
	if err != nil {
		return err
	}
	for _, str := range strs {
		req.AppendString(0, str)
	}
	return nil
}

func (r *ResultSetAdvise) NewChunk() *chunk.Chunk {
	log.Info("3333333")
	r.cnk = chunk.New([]*types.FieldType{types.NewFieldType(mysql.TypeString)}, 1, 1)
	return r.cnk
}

func (r *ResultSetAdvise) Close() error {
	log.Info("444444")
	return nil
}

func (r *ResultSetAdvise) checkTableAndColumn(databaseName string, tableName string, fieldName string) bool {
	if !r.is.TableExists(model.NewCIStr(databaseName), model.NewCIStr(tableName)) {
		return false
	}
	t, err := r.is.TableByName(model.NewCIStr(databaseName), model.NewCIStr(tableName))
	if err != nil {
		log.Error("checkTableAndColumn get table failed", zap.Error(err))
		return false
	}
	cols := make(map[string]struct{})
	for _, column := range t.Meta().Columns {
		cols[column.Name.L] = struct{}{}
	}
	fieldName = strings.ToLower(fieldName)
	_, ok := cols[fieldName]
	return ok
}

func (r *ResultSetAdvise) getRowCount(databaseName string, tableName string) (int64, error) {
	if !r.is.TableExists(model.NewCIStr(databaseName), model.NewCIStr(tableName)) {
		return 0, errors.NotFoundf("table", zap.String("database", databaseName),
			zap.String("table", tableName))
	}
	sql := "show stats_meta where db_name = '%s' and table_name = '%s';"
	sql = fmt.Sprintf(sql, databaseName, tableName)
	rows, _, err := r.session.ExecRestrictedSQL(sql)
	if err != nil {
		log.Error("getRowCount", zap.Error(err))
		return 0, err
	}
	if len(rows) == 0 {
		log.Info("getRowCount result is empty")
		return 0, err
	}
	row := rows[0]
	if row.IsEmpty() {
		log.Info("getRowCount result is empty")
		return 0, err
	}
	return row.GetInt64(5), nil
	//+-----------+------------+----------------+---------------------+--------------+-----------+
	//| Db_name   | Table_name | Partition_name | Update_time         | Modify_count | Row_count |
	//+-----------+------------+----------------+---------------------+--------------+-----------+
	//| employees | employees  |                | 2019-10-26 15:49:02 |            0 |    300024 |
	//+-----------+------------+----------------+---------------------+--------------+-----------+
}

func (r *ResultSetAdvise) getDistinctCount(databaseName string, tableName string, fieldName string) (int64, error) {
	if !r.checkTableAndColumn(databaseName, tableName, fieldName) {
		return 0, errors.NotFoundf("field", zap.String("database", databaseName),
			zap.String("table", tableName), zap.String("field", fieldName))
	}
	sql := "SHOW STATS_HISTOGRAMS where db_name = '%s' and " +
		"table_name = '%s' and " +
		"Column_name = '%s';"
	sql = fmt.Sprintf(sql, databaseName, tableName, fieldName)
	rows, _, err := r.session.ExecRestrictedSQL(sql)
	if err != nil {
		log.Error("getDistinctCount", zap.Error(err))
		return 0, err
	}
	if len(rows) == 0 {
		//
		log.Info("getRowCount result is empty")
		return 0, err
	}
	row := rows[0]
	if row.IsEmpty() {
		log.Info("getRowCount result is empty")
		return 0, err
	}
	return row.GetInt64(6), nil
	//+-----------+------------+----------------+-------------+----------+---------------------+----------------+------------+--------------+-------------+
	//| Db_name   | Table_name | Partition_name | Column_name | Is_index | Update_time         | Distinct_count | Null_count | Avg_col_size | Correlation |
	//+-----------+------------+----------------+-------------+----------+---------------------+----------------+------------+--------------+-------------+
	//| employees | employees  |                | birth_date  |        0 | 2019-10-26 15:49:01 |           4750 |          0 |           16 |    0.008148 |
	//+-----------+------------+----------------+-------------+----------+---------------------+----------------+------------+--------------+-------------+

}
