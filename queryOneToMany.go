package crvorm

import (
	"log/slog"
	"errors"
)

type QueryOneToMany struct {
	AppDb     string `json:"appDb"`
	ModelId   string `json:"modelId"`
}

func (queryOneToMany *QueryOneToMany) mergeResult(res *QueryResult, relatedRes *QueryResult, refField *Field) {
	relatedFieldName := *(refField.RelatedField)
	fieldName := refField.Field
	//将每一行的结果按照ID分配到不同的记录行上的关联字段上
	//循环结果的每行数据
	for _, relatedRow := range relatedRes.List {
		for _, row := range res.List {
			//一对多字段,关联表的关联字段存储了本表的ID，
			value, ok := row[fieldName]
			if !ok {
				value = &QueryResult{
					ModelId: *(refField.RelatedModelId),
					Total:   0,
					List:    []map[string]interface{}{},
				}
				row[fieldName] = value
			}
			//slog.Debug("mergeResult","id",row["id"],"relatedFieldName",relatedRow[relatedFieldName])
			//所以判断本表ID的值和关联表对应关联字段的值是否相等
			if row["id"] == relatedRow[relatedFieldName] {
				value.(*QueryResult).Total += 1
				value.(*QueryResult).List = append(value.(*QueryResult).List, relatedRow)
			}
		}
	}
}

func (queryOneToMany *QueryOneToMany) getFilter(parentList *QueryResult, refField *Field) *map[string]interface{} {
	//一对多字段本身是虚拟字段，需要取本表的ID字段到关联表中的关联字段查找和当前表ID字段值相同的记录
	slog.Debug("getFilter", "Filter", refField.Filter)
	//首先获取用于过滤的ID列表
	ids := GetFieldValues(parentList, "id")
	inCon := map[string]interface{}{}
	inCon[Op_in] = ids
	relatedField := *(refField.RelatedField)
	inClause := map[string]interface{}{}
	inClause[relatedField] = inCon
	if refField.Filter == nil {
		return &inClause
	}
	filter := map[string]interface{}{}
	filter[Op_and] = []interface{}{inClause, *refField.Filter}
	slog.Debug("getFilter", "filter", filter)
	return &filter
}

func (queryOneToMany *QueryOneToMany) Query(repo DataRepository, parentList *QueryResult, refField *Field) error {
	if refField.RelatedModelId == nil {
		slog.Error("One2many field must have relatedModelId", "field", refField.Field, "model", queryOneToMany.ModelId)
		return errors.New("Many2one field must have relatedModelId, field:" + refField.Field+" model:"+queryOneToMany.ModelId)
	}

	if refField.RelatedField == nil {
		slog.Error("One2many field must have RelatedField", "field", refField.Field, "model", queryOneToMany.ModelId)
		return errors.New("Many2one field must have RelatedField, field:" + refField.Field+" model:"+queryOneToMany.ModelId)
	}

	if refField.Fields == nil {
		slog.Error("One2many field must have fields", "field", refField.Field, "model", queryOneToMany.ModelId)
		return errors.New("One2many field must have fields, field:" + refField.Field+" model:"+queryOneToMany.ModelId)
	}

	if len(*refField.Fields) == 0 {
		slog.Error("One2many field must have fields", "field", refField.Field, "model", queryOneToMany.ModelId)
		return errors.New("One2many field must have fields, field:" + refField.Field+" model:"+queryOneToMany.ModelId)
	}

	filter := queryOneToMany.getFilter(parentList, refField)

	//执行查询，构造一个新的Query对象进行子表的查询，这样可以实现多层级数据表的递归查询操作
	refQueryParam := &QueryParam{
		ModelId:    *(refField.RelatedModelId),
		Pagination: refField.Pagination,
		Filter:     filter,
		Fields:     refField.Fields,
		AppDb:      queryOneToMany.AppDb,
		Sorter:     refField.Sorter,
	}
	result, err := ExecuteQuery(refQueryParam, repo, false)
	//更新查询结果到父级数据列表中
	if err != nil {
		return err
	}

	queryOneToMany.mergeResult(parentList, result, refField)
	return nil
}
