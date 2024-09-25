package crvorm

import (
	"fmt"
	"testing"
	"os"
	"log/slog"
)

func TestReplaceFilterVar(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true}))
	slog.SetDefault(logger)

	filter := &map[string]interface{}{
		"userId": "%{userId}",
		"userRoles": "%{userRoles}",
		"field1": []string{"%{filterData.model1.field1}"},
	}

	filterData := &map[string]interface{}{
		"model1": &QueryResult{
			List: []map[string]interface{}{
				map[string]interface{}{
					"field1": "value1",
				},
				map[string]interface{}{
					"field1": "value2",
				},
			},
		},
	}

	globalFilterData := &map[string]interface{}{
		"userId": "123",
		"userRoles": "role1,role2",
	}

	err:=ReplaceFilterVar(filter, filterData, globalFilterData)
	if err != nil {
		t.Errorf("ReplaceFilterVar failed")
	}

	fmt.Println("filter:", filter)
}

func TestProcessFilter(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true}))
	slog.SetDefault(logger)

	filter := &map[string]interface{}{
		"userId": map[string]interface{}{
			"Op.in":[]interface{}{"%{filterData.core_role.user.id}"},
		},
	}

	fieldType:= "many2many"
	relatedModelId:= "core_user"
	filterData := &[]FilterDataItem{
		{
			ModelId: "core_role",
			Filter: &map[string]interface{}{
				"id": map[string]interface{}{
					"Op.in": []interface{}{"%{userRoles}"},
				},
			},
			Fields: &[]Field{
				{
					Field: "id",
				},
				{
					Field: "user",
					FieldType:&fieldType, 
					RelatedModelId: &relatedModelId,
					Fields: &[]Field{
						{
							Field: "id",
						},
					},
				},
			},
		},
	}

	globalFilterData := &map[string]interface{}{
		"userRoles": "admin\",\"admin",
	}

	dbConf:= &DbConf{
		Server:"192.168.1.43:3306",
        Password:"4576_Iee0787",
        User:"root",
        DbName:"docanalysis",
        ConnMaxLifetime:3,
        MaxOpenConns:10,
        MaxIdleConns:10,
	}

	orm:=&CrvOrm{}
	err:=orm.InitDefaultRepo(dbConf)
	if err!=nil{
		t.Errorf("InitDefaultRepo failed\n")
	}

	err=ProcessFilter(filter, filterData, globalFilterData, "docanalysis", orm.Repo)
	if err != nil {
		t.Errorf("ProcessFilter failed\n")
	}

	fmt.Println("filter:", filter)
}