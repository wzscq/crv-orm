package crvorm

import (
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
)

type DefaultOperInConvert struct {
	ModelId string   `json:"modelId"`
	Fields  *[]Field `json:"fields"`
}

func (opc *DefaultOperInConvert) Convert(op string, field string, value interface{}) (string, interface{}, error) {
	//查看当前字段是否是many2many字段
	if opc.Fields != nil {
		for _, fieldItem := range *opc.Fields {
			if fieldItem.Field == field && fieldItem.FieldType != nil && *fieldItem.FieldType == FIELDTYPE_MANY2MANY {
				//对字段的值做转换，改为一个子查询字符串
				var err error
				value, err = opc.convertMany2manyValue(opc.ModelId, &fieldItem, value)
				if err != nil {
					return field, value, err
				}
				field = "id"
			}

			if fieldItem.Field == field && fieldItem.FieldType != nil && *fieldItem.FieldType == FIELDTYPE_ONE2MANY {
				//对字段的值做转换，改为一个in查询
				//一对多虚拟字段，转换为对ID的过滤
				field = "id"
			}
		}
	}

	return field, value, nil
}

func (opc *DefaultOperInConvert) convertMany2manyValue(modelId string, field *Field, value interface{}) (string, error) {
	if field.RelatedModelId == nil {
		slog.Error("convertMany2manyValue the many2many field has not related model id", "field", field.Field)
		return "", errors.New("the many2many field has not related model id")
	}

	var sVal string
	switch value.(type) {
	case []string:
	case []interface{}:
		sliceVal := value.([]interface{})
		sVal = opc.joinSlice(sliceVal, ",")
	default:
		slog.Error("convertMany2manyValue not supported value type", "val type", reflect.TypeOf(value))
		return "", errors.New("convertMany2manyValue not supported value type " + reflect.TypeOf(value).String())
	}

	associationModelId := opc.getRelatedModelID(modelId, *field.RelatedModelId, field.AssociationModelId)
	subSelect := "select " + modelId + "_id as id from " + associationModelId + " where " + *field.RelatedModelId + "_id in (" + sVal + ")"
	return subSelect, nil
}

func (opc *DefaultOperInConvert) getRelatedModelID(
	modelID string,
	relatedModelID string,
	associationModelID *string) string {

	if associationModelID != nil {
		return *associationModelID
	}

	if modelID >= relatedModelID {
		return relatedModelID + "_" + modelID
	}
	return modelID + "_" + relatedModelID
}

func (opc *DefaultOperInConvert) joinSlice(sliceVal []interface{}, split string) string {
	values := ""
	for _, val := range sliceVal {
		slog.Debug("joinSlice val type", "val type", reflect.TypeOf(val))

		switch val.(type) {
		case string:
			sVal, _ := val.(string)
			values = values + "'" + opc.replaceApostrophe(sVal) + "',"
		case float64:
			f64Val, _ := val.(float64)
			sVal := strconv.FormatFloat(f64Val, 'f', -1, 64)
			values = values + sVal + ","
		}
	}

	if len(values) > 1 {
		values = values[0 : len(values)-1]
	}

	return values
}

func (opc *DefaultOperInConvert) replaceApostrophe(str string) string {
	replacedStr := strings.ReplaceAll(str, "'", "''")
	return replacedStr
}
