package crvorm

import (
	"log/slog"
	"strconv"
	"strings"
	"errors"
)

type Sorter struct {
	Field  string    `json:"field"`
	Order  string    `json:"order"`
	Values *[]string `json:"values,omitempty"`
}

type SQLParam struct {
	AppDb     string `json:"appDb"`
	ModelId   string `json:"modelId"`
	Fields    string `json:"fields"`
	Where     string `json:"where"`
	Limit     string `json:"limit"`
	Sorter    string `json:"sorter"`
	Summarize string `json:"summarize"`
}

type Pagination struct {
	Current  int `json:"current"`
	PageSize int `json:"pageSize"`
}

type Field struct {
	Field              string                  `json:"field"`
	DataType           *string                 `json:"dataType,omitempty"`
	FieldType          *string                 `json:"fieldType,omitempty"`
	RelatedModelId     *string                 `json:"relatedModelId,omitempty"`
	RelatedField       *string                 `json:"relatedField,omitempty"`
	AssociationModelId *string                 `json:"associationModelId,omitempty"`
	Pagination         *Pagination             `json:"pagination,omitempty"`
	Filter             *map[string]interface{} `json:"filter,omitempty"`
	Fields             *[]Field                `json:"fields,omitempty"`
	Sorter             *[]Sorter               `json:"sorter,omitempty"`
	Summarize          *string                 `json:"summarize,omitempty"`
}

type QueryParam struct {
	AppDb      string				   `json:"appDb"`
	ModelId    string                  `json:"modelId"`
	Filter     *map[string]interface{} `json:"filter,omitempty"`
	Fields     *[]Field                `json:"fields"`
	Sorter     *[]Sorter               `json:"sorter,omitempty"`
	Pagination *Pagination             `json:"pagination,omitempty"`
}

type QueryResult struct {
	ModelId   string                   `json:"modelId"`
	Value     *string                  `json:"value,omitempty"`
	Total     int                      `json:"total"`
	Summaries map[string]interface{}  `json:"summaries,omitempty"`
	List      []map[string]interface{} `json:"list,omitempty"`
}

func QueryToSQLPARAM(query *QueryParam) (*SQLParam, error) {
	sqlParam := &SQLParam{
		AppDb:     query.AppDb,
		ModelId:   query.ModelId,
		Fields:    "",
		Where:     "",
		Limit:     "",
		Sorter:    "",
		Summarize: "",
	}
	//处理fields
	sqlParam.Fields = GetQueryFields(query.Fields)
	//处理汇总列
	sqlParam.Summarize = GetSummarizeFields(query.Fields)
	//处理filter
	var err error
	opc := &DefaultOperInConvert{
		ModelId: query.ModelId,
		Fields:  query.Fields,
	}
	fc := &FilterConverter{
		OperInConvert: opc,
	}
	sqlParam.Where, err = fc.FilterToSQLWhere(query.Filter)
	if err != nil {
		return nil, err
	}
	//处理sorter
	sqlParam.Sorter = GetQuerySorter(query.Sorter)
	//处理pagination
	sqlParam.Limit = GetQueryLimit(query.Pagination)
	return sqlParam, nil
}

func GetQueryFields(fields *[]Field) string {
	fieldsStr := ""
	for _, field := range *fields {
		if field.FieldType == nil {
			fieldsStr = fieldsStr + field.Field + ","
		} else {
			if *(field.FieldType) != FIELDTYPE_MANY2MANY &&
				*(field.FieldType) != FIELDTYPE_ONE2MANY &&
				*(field.FieldType) != FIELDTYPE_FILE {
					fieldsStr = fieldsStr + field.Field + ","
			}
		}
	}
	fieldsStr = fieldsStr[0 : len(fieldsStr)-1]
	return fieldsStr
}

func GetSummarizeFields(fields *[]Field) string {
	var summarizeFields string
	for _, field := range *fields {
		if field.Summarize != nil && len(*field.Summarize) > 0 {
			summarizeFields = summarizeFields + *field.Summarize + " as " + field.Field + ","
		}
	}
	return summarizeFields
}

func GetQuerySorter(sorters *[]Sorter) string {
	if sorters == nil || len(*(sorters)) == 0 {
		return " id asc "
	}

	var sorterStr string
	for _, sorter := range *(sorters) {
		if sorter.Values != nil && len(*sorter.Values) > 0 {
			sorterStr = sorterStr + "FIELD(" + sorter.Field + ",'" + strings.Join(*sorter.Values, "','") + "') " + sorter.Order + ","
		} else {
			sorterStr = sorterStr + sorter.Field + " " + sorter.Order + ","
		}
	}

	sorterStr = sorterStr[0 : len(sorterStr)-1]
	return sorterStr
}

func GetQueryLimit(pagination *Pagination) string {
	//如果没有提供分页信息，这里暂时给一个固定值，避免数据量过大造成性能或内存问题
	if pagination == nil {
		return "0,1000"
	}

	if pagination.PageSize < 0 || pagination.Current <= 0 {
		slog.Error("GetQueryLimit pageSize and current must great than 0", "Pagination", pagination)
		return "0,0"
	}

	row := strconv.Itoa((pagination.Current - 1) * pagination.PageSize)
	count := strconv.Itoa(pagination.PageSize)
	limit := row + "," + count

	return limit
}

func SQLParamToSummarizeSQL(sqlParam *SQLParam) string {
	sql := "select " + sqlParam.Summarize + " count(*) as __count" +
		" from " + sqlParam.AppDb + "." + sqlParam.ModelId +
		" where " + sqlParam.Where
	return sql
}

func SQLParamToDataSQL(sqlParam *SQLParam) (string) {
	sql := "select " + sqlParam.Fields +
		" from " + sqlParam.AppDb + "." + sqlParam.ModelId +
		" where " + sqlParam.Where +
		" order by " + sqlParam.Sorter +
		" limit " + sqlParam.Limit
	return sql
}

func ExecuteQuery(queryParam *QueryParam,repo DataRepository,withSummarize bool) (*QueryResult, error) {
	sqlParam, err := QueryToSQLPARAM(queryParam)
	if err != nil {
		slog.Error("QueryToSQLPARAM failed", "error", err)
		return nil,err
	}

	result:=&QueryResult{
		ModelId:   queryParam.ModelId,
		Total: -1,
	}

	if withSummarize==true {
		sql := SQLParamToSummarizeSQL(sqlParam)
		summaries, err := repo.Query(sql)
		if err != nil {
			slog.Error("Query failed", "error", err)
			return nil,err
		}

		if len(summaries) <= 0 {
			slog.Error("getCountAndSummaries with empty result", "summaries", summaries)
			return nil, errors.New("getCountAndSummaries with empty result sql:" + sql) 
		}

		slog.Debug("getCountAndSummaries", "summaries", summaries)
		result.Summaries = summaries[0]
		result.Total = int(result.Summaries["__count"].(int64))
		delete(result.Summaries, "__count")
	}

	if result.Total != 0 && (queryParam.Pagination==nil || queryParam.Pagination.PageSize > 0) {
		sql := SQLParamToDataSQL(sqlParam)
		data, err := repo.Query(sql)
		if err != nil {
			slog.Error("Query failed", "error", err)
			return nil, err
		}
		result.List = data

		if result.Total <= 0 {
			result.Total = len(data)
		}

		//循环所有字段，对每个关联字段进行处理
		for _, field := range *(queryParam.Fields) {
			//由于MANY_TO_MANY和ONE_TO_MANY字段本身不对应实际数据库表中的字段，
			//需要单独处理，所以先将这两个类型的字段过滤掉
			if field.FieldType != nil {
				slog.Debug("fieldType", "fieldType", *field.FieldType, "field", field.Field)
				relatedQuery:= GetRelatedModelQuerier(queryParam.AppDb,queryParam.ModelId,*field.FieldType)
				err:=relatedQuery.Query(repo, result, &field)
				if err != nil {
					slog.Error("Query relatedmodel failed", "error", err, "field", field.Field, "model", queryParam.ModelId)
					return nil, err
				}
			}
		}
	}

	return result, nil
}
