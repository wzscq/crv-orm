package crvorm

import (
	"fmt"
	"testing"
)

var fields = &[]Field{
	{
		Field: "id",
	},
	{
		Field: "name",
	},
}

func TestQueryToSQLPARAM(t *testing.T) {
	query := &QueryParam{
		Fields:  fields,
		ModelId: "user",
		Filter: &map[string]interface{}{
			"name": "test",
			"age": map[string]interface{}{
				"Op.gt": 20,
				"Op.lt": "50",
			},
			"sex": map[string]interface{}{
				"Op.and": []interface{}{
					map[string]interface{}{
						"Op.in": []interface{}{"1"},
					},
					map[string]interface{}{
						"Op.eq": "0",
					},
				},
			},
			"id": map[string]interface{}{
				"Op.in": []interface{}{
					"2",
					"3",
				},
			},
		},
		Sorter: nil,
		Pagination: &Pagination{
			PageSize: 10,
			Current:  1,
		},
	}

	sqlParam, err := QueryToSQLPARAM(query)
	if err != nil {
		t.Errorf("QueryToSQLPARAM failed")
	}

	fmt.Println("modelId:", sqlParam.ModelId)
	fmt.Println("fields:", sqlParam.Fields)
	fmt.Println("where:", sqlParam.Where)
	fmt.Println("limit:", sqlParam.Limit)
	fmt.Println("sorter:", sqlParam.Sorter)

	sql := SQLParamToSummarizeSQL(sqlParam)
	fmt.Println(sql)

	sql = SQLParamToDataSQL(sqlParam)
	fmt.Println(sql)
}
