package crvorm

import (
	"fmt"
	"testing"
	"os"
	"log/slog"
	"encoding/json"
)

func _TestExecuteQuery(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true}))
	slog.SetDefault(logger)

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
		t.Errorf("InitDefaultRepo failed")
	}

	many2many:="many2many"
	relatedModelId:= "core_role"
	query := &QueryParam{
		AppDb: "docanalysis",
		Fields:&[]Field{
			{
				Field: "id",
			},
			{
				Field: "user_name_zh",
			},
			{
				Field: "roles",
				FieldType:&many2many,
				RelatedModelId:&relatedModelId,
				Fields:&[]Field{
					{
						Field: "id",
					},
				},
			},
		},
		ModelId: "core_user",
		Filter: &map[string]interface{}{
			"id":"admin",
		},
		Sorter: nil,
		Pagination: &Pagination{
			PageSize: 10,
			Current:  1,
		},
	}

	res,err:=orm.ExecuteQuery(query)
	if err != nil {
		t.Errorf("QueryToSQLPARAM failed")
	}

	resStr,_:=json.Marshal(res)

	fmt.Println("data:", string(resStr))
}