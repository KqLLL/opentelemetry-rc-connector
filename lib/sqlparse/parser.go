package sqlparse

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

var joinTypeMap = map[ast.JoinType]string{
	ast.CrossJoin: "CrossJoin",
	ast.LeftJoin:  "LeftJoin",
	ast.RightJoin: "RightJoin",
}

type SqlParse struct {
	Tables    []string
	Operation string
	JoinType  string
}

func parseQuery(sqlExpression string) (*ast.StmtNode, error) {
	p := parser.New()

	statements, _, err := p.ParseSQL(sqlExpression)
	if err != nil {
		return nil, err
	}

	return &statements[0], nil
}

func (sp *SqlParse) Enter(node ast.Node) (ast.Node, bool) {
	switch n := node.(type) {
	case *ast.TableName:
		sp.Tables = append(sp.Tables, n.Name.O)
	case *ast.Join:
		if tp, ok := joinTypeMap[n.Tp]; ok {
			sp.JoinType = tp
		}
	}

	return node, false
}

func (sp *SqlParse) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

func (sp *SqlParse) Parse(node *ast.StmtNode) *SqlParse {
	sp.Operation = ast.GetStmtLabel(*node)
	(*node).Accept(sp)
	return sp
}

func ExtractSQL(sql string) (*SqlParse, error) {
	parsedNode, err := parseQuery(sql)
	if err != nil {
		fmt.Printf("Error parsing SQL: %v\n", err)
		return nil, err
	}
	sqlParser := SqlParse{}
	parsingResult := sqlParser.Parse(parsedNode)

	return parsingResult, nil
}
