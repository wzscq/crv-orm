package crvorm

import (
	//"log/slog"
)

const (
	FIELDTYPE_MANY2MANY = "many2many"
	FIELDTYPE_MANY2ONE  = "many2one"
	FIELDTYPE_ONE2MANY  = "one2many"
	FIELDTYPE_FILE      = "file"
)

type QueryRelatedModel interface {
	Query(repo DataRepository, parentList *QueryResult, refField *Field) error
}

func GetRelatedModelId(
	modelId string,
	relatedModelId string,
	associationModelId *string) string {

	if associationModelId != nil {
		return *associationModelId
	}

	if modelId >= relatedModelId {
		return relatedModelId + "_" + modelId
	}
	return modelId + "_" + relatedModelId
}

func GetRelatedModelQuerier(appDb string, modelId string,fieldType string) QueryRelatedModel {
	if fieldType == FIELDTYPE_MANY2MANY {
		return &QueryManyToMany{
			AppDb:     appDb,
			ModelId:   modelId,
		}
	} else if fieldType == FIELDTYPE_ONE2MANY {
		return &QueryOneToMany{
			AppDb:     appDb,
			ModelId:   modelId,
		}
	} else if fieldType == FIELDTYPE_MANY2ONE {
		return &QueryManyToOne{
			AppDb:     appDb,
			ModelId:   modelId,
		}
	} else if fieldType == FIELDTYPE_FILE {
		return &QueryFile{
			AppDb:     appDb,
			ModelId:   modelId,
		}
	}
	return nil
}

func GetFieldValues(res *QueryResult, fieldName string) []string {
	var valList []string
	for _, row := range res.List {
		if row[fieldName] != nil {
			//slog.Info("Field value: ","fieldName",fieldName, "value",row[fieldName])
			sVal := row[fieldName].(string)
			valList = append(valList, sVal)
		}
	}
	return valList
}
