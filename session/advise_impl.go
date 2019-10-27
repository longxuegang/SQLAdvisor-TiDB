package session

import (
	"bytes"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/opcode"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"go.uber.org/zap"
)

const SingleIndexThreshold = 100
const MutiIndexThreshold = 10000

type TableInfo struct {
	TableNameList []string
	ExprNode      ast.ExprNode
	GroupByField  []string
	OrderByInfo   []OrderByInfo
}

type OrderByInfo struct {
	OrderByField []string
	OrderByDesc  bool
}

type Target struct {
	DatabaseName string
	TableName    string
	Field        string
	AvgScanNum   float64
}

func GetAvgScanNum(r *ResultSetAdvise, databaseName string, tableName string, field string) (float64, error) {
	//rowCount, err := r.getRowCount(databaseName, tableName)
	//if err != nil {
	//	return 0, errors.Trace(err)
	//}
	//disCount, err := r.getDistinctCount(databaseName, tableName, field)
	//if err != nil {
	//	return 0, errors.Trace(err)
	//}
	//log.Info("GetAvgScanNum",
	//	zap.Int64("rowCount", rowCount),
	//	zap.Int64("disCount", disCount))
	//if disCount == 0 {
	//	return math.MaxFloat64, nil
	//}
	//return float64(rowCount / disCount), nil
	return 1, nil
}

func GetIndexAdviser(r *ResultSetAdvise, adviseVisitor *AdviseVisitor) ([]Target, []string, error) {
	var targetList []Target
	tableName := adviseVisitor.tableName
	for _, enode := range adviseVisitor.whereExpr {
		fieldName := enode.(*ast.BinaryOperationExpr).L.(*ast.ColumnNameExpr).Name.Name.O
		avgScanNum, err := GetAvgScanNum(r, adviseVisitor.databaseName, tableName, fieldName)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if avgScanNum < SingleIndexThreshold {
			if enode.(*ast.BinaryOperationExpr).Op != opcode.EQ {
				avgScanNum += 300000
			}

			targetList = append(targetList, Target{
				DatabaseName: adviseVisitor.databaseName,
				TableName:    tableName,
				Field:        fieldName,
				AvgScanNum:   avgScanNum,
			})
		} else if avgScanNum >= SingleIndexThreshold && avgScanNum < MutiIndexThreshold {
			if enode.(*ast.BinaryOperationExpr).Op != opcode.EQ {
				avgScanNum += 300000
			}

			targetList = append(targetList, Target{
				DatabaseName: adviseVisitor.databaseName,
				TableName:    tableName,
				Field:        fieldName,
				AvgScanNum:   avgScanNum})
		} else {
			continue
		}
	}
	if len(adviseVisitor.groupByField) != 0 {
		for _, field := range adviseVisitor.groupByField {
			avgScanNum, err := GetAvgScanNum(r, adviseVisitor.databaseName, tableName, field)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			if avgScanNum < SingleIndexThreshold {
				avgScanNum += 100000
				targetList = append(targetList, Target{
					DatabaseName: adviseVisitor.databaseName,
					TableName:    tableName,
					Field:        field, AvgScanNum: avgScanNum})
			} else if avgScanNum >= SingleIndexThreshold && avgScanNum < MutiIndexThreshold {
				avgScanNum += 100000
				targetList = append(targetList, Target{
					DatabaseName: adviseVisitor.databaseName,
					TableName:    tableName,
					Field:        field, AvgScanNum: avgScanNum})
			} else {
				panic("2222")
			}
		}
	}
	if len(adviseVisitor.orderByInfo) != 0 {
		sum := 1
		for _, info := range adviseVisitor.orderByInfo {
			if info.OrderByDesc {
				sum++
			}
		}
		if sum != len(adviseVisitor.orderByInfo) {
			return targetList, nil, nil
		}
		for _, info := range adviseVisitor.orderByInfo {
			for _, v := range info.OrderByField {
				log.Info("order field",zap.String("f",v))
				avgScanNum, err := GetAvgScanNum(r, adviseVisitor.databaseName, tableName, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				if avgScanNum < SingleIndexThreshold {
					avgScanNum += 200000
					targetList = append(targetList, Target{
						DatabaseName: adviseVisitor.databaseName,
						TableName:    tableName, Field: v, AvgScanNum: avgScanNum})
				} else if avgScanNum >= SingleIndexThreshold && avgScanNum < MutiIndexThreshold {
					avgScanNum += 200000
					targetList = append(targetList, Target{
						DatabaseName: adviseVisitor.databaseName,
						TableName:    tableName, Field: v, AvgScanNum: avgScanNum})
				} else {
					panic("2222")
				}
			}
		}
	}
	return targetList, nil, nil
}

type tname struct {
	d string
	t string
	f string
}

func advise(r *ResultSetAdvise, defaultDatabase string, stmt *ast.SelectStmt) ([]string, error) {
	v := &AdviseVisitor{
		defaultDatabase: defaultDatabase,
	}
	stmt.Accept(v)
	tlist, _, err := GetIndexAdviser(r, v)
	if err != nil {
		return nil, err
	}
	rm := make(map[tname]float64)
	for _, t := range tlist {
		dt := tname{t.DatabaseName, t.TableName, t.Field}
		if rr, ok := rm[dt]; ok {
			if rr > t.AvgScanNum {
				rm[dt] = t.AvgScanNum
			}
		} else {
			rm[dt] = t.AvgScanNum
		}
	}
	var result []Target
	for k, v := range rm {
		result = append(result, Target{DatabaseName: k.d, TableName: k.t, Field: k.f, AvgScanNum: v})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].AvgScanNum < result[j].AvgScanNum
	})
	log.Info("advise", zap.Reflect("result", result))
	resultMap := make(map[tname][]Target)
	for _, r := range result {
		k := tname{d: r.DatabaseName, t: r.TableName}
		if v, ok := resultMap[k]; ok {
			if len(v) > 0 && r.AvgScanNum >= 300000 {
				avg := v[len(v)-1].AvgScanNum
				if avg >= 200000 && avg < 300000 {
					continue
				}
			}
			if len(v) <= 3 {
				v = append(v, r)
				resultMap[k] = v
			}
		} else {
			resultMap[k] = []Target{r}
		}
	}
	var rstr []string
	for _, v := range resultMap {
		s, err := createIndexSQL(v)
		if err != nil {
			return nil, err
		}
		rstr = append(rstr, s)
	}
	return rstr, nil
}

type AdviseVisitor struct {
	defaultDatabase string
	databaseName    string
	tableName       string
	whereExpr       []ast.ExprNode
	groupByField    []string
	orderByInfo     []OrderByInfo
}

func (a *AdviseVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n := n.(type) {
	case *ast.SelectStmt:
		// get where expr
		if n.Where != nil {
			v := &WhereVisitor{}
			n.Where.Accept(v)
			a.whereExpr = v.exprList
		}
	case *ast.Join:
		if n.Left == nil {
			return n, true
		}
		if s, ok := n.Left.(*ast.TableSource); ok {
			if s, ok := s.Source.(*ast.TableName); ok {
				a.databaseName = s.Schema.O
				a.tableName = s.Name.O
				if a.databaseName == "" {
					a.databaseName = a.defaultDatabase
				}
				log.Info("find table", zap.String("database", a.databaseName), zap.String("table", a.tableName))
			}
		}
		return n, true
	case *ast.GroupByClause:
		for _, v := range n.Items {
			if v, ok := v.Expr.(*ast.ColumnNameExpr); ok {
				a.groupByField = append(a.groupByField, v.Name.Name.O)
			}
		}
	case *ast.OrderByClause:
		for _, v := range n.Items {
			o := OrderByInfo{}
			if c, ok := v.Expr.(*ast.ColumnNameExpr); ok {
				o.OrderByField = append(o.OrderByField, c.Name.Name.O)
				o.OrderByDesc = v.Desc
				a.orderByInfo = append(a.orderByInfo, o)
			}
		}
		log.Info("no orderby?", zap.Reflect("f", a.orderByInfo))
	}
	return n, false
}

func createIndexSQL(target []Target) (string, error) {
	databaseName := ""
	tableName := ""
	indexName := "idx"
	var indexs []*ast.IndexColName
	for _, t := range target {
		databaseName = t.DatabaseName
		tableName = t.TableName
		indexName = indexName + "_" + t.Field
		index := &ast.IndexColName{
			Length: -1,
			Column: &ast.ColumnName{
				Name: model.NewCIStr(t.Field),
			},
		}
		indexs = append(indexs, index)
	}
	stmt := ast.AlterTableStmt{
		Table: &ast.TableName{
			Schema: model.NewCIStr(databaseName),
			Name:   model.NewCIStr(tableName),
		},
		Specs: []*ast.AlterTableSpec{
			{
				Tp:      ast.AlterTableAddConstraint,
				FromKey: model.NewCIStr(""),
				ToKey:   model.NewCIStr(""),
				Constraint: &ast.Constraint{
					Name: indexName,
					Tp:   ast.ConstraintIndex,
					Keys: indexs,
				},
			},
		},
	}
	buf := new(bytes.Buffer)
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
	err := stmt.Restore(ctx)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (a *AdviseVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

type WhereVisitor struct {
	exprList []ast.ExprNode
}

func (w *WhereVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	if n, ok := n.(*ast.BinaryOperationExpr); ok {
		if _, ok := n.L.(*ast.ColumnNameExpr); ok {
			w.exprList = append(w.exprList, n)
			return n, false
		}
		if n.Op == opcode.LogicOr {
			return n, true
		}
	}
	return n, false
}

func (w *WhereVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

type TableNameVisitor struct {
	exprList []ast.ExprNode
}

func (w *TableNameVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	if n, ok := n.(*ast.BinaryOperationExpr); ok {
		if _, ok := n.L.(*ast.ColumnNameExpr); ok {
			w.exprList = append(w.exprList, n)
			return n, false
		}
		if n.Op == opcode.LogicOr {
			return n, true
		}
	}
	return n, false
}

func (w *TableNameVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

type FromVisitor struct {
	exprList     []ast.ExprNode
	tableName    string
	databaseName string
}

func (w *FromVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	if n, ok := n.(*ast.Join); ok {
		if n.Left == nil {
			return nil, true
		}
		if s, ok := n.Left.(*ast.TableName); ok {
			w.databaseName = s.Schema.O
			w.tableName = s.Name.O
		}
		return nil, true
	}
	return n, false
}

func (w *FromVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

//func splitSQL(stmts []*ast.SelectStmt) []*ast.SelectStmt {
//	var result []*ast.SelectStmt
//	for _, stmt := range stmts {
//		tp := stmt.From.TableRefs.Tp
//		on := stmt.From.TableRefs.On
//		switch tp {
//		case ast.LeftJoin:
//
//		case ast.RightJoin:
//		case ast.CrossJoin:
//		}
//	}
//	return result
//}
