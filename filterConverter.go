package crvorm

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
)

/**
过滤条件的配置方式参考了sequelize框架的格式，具体可参考网址：https://sequelize.org/v7/manual/model-querying-basics.html
以下为全部语法格式，这里不是所有的都支持了，需要逐步完善
{
    [Op.and]: [{ a: 5 }, { b: 6 }],            // (a = 5) AND (b = 6)
    [Op.or]: [{ a: 5 }, { b: 6 }],             // (a = 5) OR (b = 6)
    filename: {
      // Basics
      [Op.eq]: 3,                              // = 3
      [Op.ne]: 20,                             // != 20
      [Op.is]: null,                           // IS NULL
      [Op.not]: true,                          // IS NOT TRUE
      [Op.or]: [5, 6],                         // (someAttribute = 5) OR (someAttribute = 6)

      // Using dialect specific column identifiers (PG in the following example):
      [Op.col]: 'user.organization_id',        // = "user"."organization_id"

      // Number comparisons
      [Op.gt]: 6,                              // > 6
      [Op.gte]: 6,                             // >= 6
      [Op.lt]: 10,                             // < 10
      [Op.lte]: 10,                            // <= 10
      [Op.between]: [6, 10],                   // BETWEEN 6 AND 10
      [Op.notBetween]: [11, 15],               // NOT BETWEEN 11 AND 15

      // Other operators

      [Op.all]: sequelize.literal('SELECT 1'), // > ALL (SELECT 1)

      [Op.in]: [1, 2],                         // IN [1, 2]
      [Op.notIn]: [1, 2],                      // NOT IN [1, 2]

      [Op.like]: '%hat',                       // LIKE '%hat'
      [Op.notLike]: '%hat',                    // NOT LIKE '%hat'
      [Op.startsWith]: 'hat',                  // LIKE 'hat%'
      [Op.endsWith]: 'hat',                    // LIKE '%hat'
      [Op.substring]: 'hat',                   // LIKE '%hat%'
      [Op.iLike]: '%hat',                      // ILIKE '%hat' (case insensitive) (PG only)
      [Op.notILike]: '%hat',                   // NOT ILIKE '%hat'  (PG only)
      [Op.regexp]: '^[h|a|t]',                 // REGEXP/~ '^[h|a|t]' (MySQL/PG only)
      [Op.notRegexp]: '^[h|a|t]',              // NOT REGEXP/!~ '^[h|a|t]' (MySQL/PG only)
      [Op.iRegexp]: '^[h|a|t]',                // ~* '^[h|a|t]' (PG only)
      [Op.notIRegexp]: '^[h|a|t]',             // !~* '^[h|a|t]' (PG only)

      [Op.any]: [2, 3],                        // ANY ARRAY[2, 3]::INTEGER (PG only)
      [Op.match]: Sequelize.fn('to_tsquery', 'fat & rat') // match text search for strings 'fat' and 'rat' (PG only)

      // In Postgres, Op.like/Op.iLike/Op.notLike can be combined to Op.any:
      [Op.like]: { [Op.any]: ['cat', 'hat'] }  // LIKE ANY ARRAY['cat', 'hat']
	}
*/

const (
	Op_and        = "Op.and"
	Op_or         = "Op.or"
	Op_eq         = "Op.eq"
	Op_ne         = "Op.ne"
	Op_is         = "Op.is"
	Op_not        = "Op.not"
	Op_gt         = "Op.gt"
	Op_gte        = "Op.gte"
	Op_lt         = "Op.lt"
	Op_lte        = "Op.lte"
	Op_between    = "Op.between"
	Op_notBetween = "Op.notBetween"
	Op_like       = "Op.like"
	Op_in         = "Op.in"
	Op_notIn      = "Op.notIn"
)

// 当操作符为In时，允许对过滤的字段和值进行转换处理的接口
type OperInConvert interface {
	Convert(op string, field string, value interface{}) (string, interface{}, error)
}

type FilterConverter struct {
	OperInConvert OperInConvert
}

func (fc *FilterConverter) FilterToSQLWhere(filter *map[string]interface{}) (string, error) {
	var str string
	var err error
	var where string
	if filter != nil {
		for key, value := range *filter {
			switch key {
			case Op_or:
				mVal, _ := value.([]interface{})
				slog.Debug("FilterToSQLWhere", "key", key, "value", value)
				str, err = fc.convertArrayFilter("or", mVal)
			case Op_and:
				slog.Debug("FilterToSQLWhere", "key", key, "value", value)
				mVal, _ := value.([]interface{})
				str, err = fc.convertArrayFilter("and", mVal)
			default:
				slog.Debug("FilterToSQLWhere", "key", key, "value", value)
				str, err = fc.convertFieldFilter(key, value)
			}

			if err != nil {
				return "", err
			}

			where = where + " (" + str + ") and"
		}
	}

	if len(where) > 0 {
		where = where[0 : len(where)-3]
	} else {
		where = "1=1"
	}
	return where, nil
}

func (fc *FilterConverter) convertArrayFilter(logicOp string, value []interface{}) (string, error) {
	if len(value) == 0 {
		slog.Error("convertArrayFilter error,filter format error", "logicOp", logicOp, "value", value)
		return "", errors.New("convertArrayFilter error, filter format error")
	}

	var where string = ""
	var str string
	var err error
	for _, v := range value {
		//每个行应该是一个对象
		mVal, _ := v.(map[string]interface{})
		//slog.Debug("convertArrayFilter", "mVal", mVal)
		str, err = fc.FilterToSQLWhere(&mVal)
		//slog.Debug("convertArrayFilter", "str", str)
		if err != nil {
			return "", err
		}
		where = where + " (" + str + ") " + logicOp
	}

	where = where[0 : len(where)-len(logicOp)]

	return where, nil
}

/*
字段类型值的过滤，字段值有三种类型的过滤条件
{fieldname:value}  =>  fieldname=value  //直接给值，相当与Op.like操作符
{fieldname:[val1,val2]} => fieldname in (val1,val2)  //数据，相当于Op.in操作符？
{fieldname:{Op.gt,value}} => filename > value  //明确给出操作符，按照操作符来解析
*/
func (fc *FilterConverter) convertFieldFilter(field string, value interface{}) (string, error) {
	switch value.(type) {
	case string:
		sVal, _ := value.(string)
		return fc.convertFieldValueString(" like ", field, sVal), nil
	case float64:
		fVal, _ := value.(float64)
		sVal := fmt.Sprintf("%f", fVal)
		return fc.convertFieldValueString(" = ", field, sVal), nil
	case int64:
		iVal, _ := value.(int64)
		sVal := fmt.Sprintf("%d", iVal)
		return fc.convertFieldValueString(" = ", field, sVal), nil
	case map[string]interface{}:
		mVal, _ := value.(map[string]interface{})
		return fc.convertFieldValueMap(field, mVal)
	case nil:
		return fc.convertFieldValueNull(" is ", field), nil
	case []interface{}:
		sliceVal := value.([]interface{})
		return fc.convertFieldValueArray(" in ", field, sliceVal), nil
	default:
		slog.Error("convertFieldFilter not supported field filter type", "type", reflect.TypeOf(value))
		errorStr := fmt.Sprintf("not supported field filter value type field: %s  value type %v ", field, value)
		return "", errors.New(errorStr)
	}
}

func (fc *FilterConverter) convertFieldValueNull(op string, field string) string {
	return field + op + " null "
}

func (fc *FilterConverter) convertFieldValueString(op string, field string, value string) string {
	if op == " like " {
		value = "%" + value + "%"
	}
	return field + op + "'" + fc.replaceApostrophe(value) + "'"
}

func (fc *FilterConverter) convertFieldValueStringArray(op string, field string, sliceVal []string) string {
	//slog.Debug("convertFieldValueStringArray", "op", op, "field", field, "sliceVal", sliceVal)
	values := ""
	for _, sVal := range sliceVal {
		values = values + "'" + fc.replaceApostrophe(sVal) + "',"
	}
	values = values[0 : len(values)-1]
	return field + op + "(" + values + ")"
}

func (fc *FilterConverter) convertFieldValueArray(op string, field string, sliceVal []interface{}) string {
	values := ""
	for _, val := range sliceVal {
		slog.Debug("convertFieldValueArray val type", "val type", reflect.TypeOf(val))

		switch val.(type) {
		case string:
			sVal, _ := val.(string)
			values = values + "'" + fc.replaceApostrophe(sVal) + "',"
		case float64:
			f64Val, _ := val.(float64)
			sVal := strconv.FormatFloat(f64Val, 'f', -1, 64)
			values = values + sVal + ","
		}
	}

	if len(values) > 1 {
		values = values[0 : len(values)-1]
	}
	return field + op + "(" + values + ")"
}

func (fc *FilterConverter) convertFieldOpNormal(op string, field string, value interface{}) (string, error) {
	switch value.(type) {
	case string:
		sVal, _ := value.(string)
		return fc.convertFieldValueString(op, field, sVal), nil
	case []string:
		sliceVal := value.([]string)
		return fc.convertFieldValueStringArray(op, field, sliceVal), nil
	case int:
		iVal, _ := value.(int)
		sVal := fmt.Sprintf("%d", iVal)
		return fc.convertFieldValueString(op, field, sVal), nil
	case int64:
		iVal, _ := value.(int64)
		sVal := fmt.Sprintf("%d", iVal)
		return fc.convertFieldValueString(op, field, sVal), nil
	case float64:
		fVal, _ := value.(float64)
		sVal := fmt.Sprintf("%f", fVal)
		return fc.convertFieldValueString(op, field, sVal), nil
	case []interface{}:
		sliceVal := value.([]interface{})
		return fc.convertFieldValueArray(op, field, sliceVal), nil
	case nil:
		return fc.convertFieldValueNull(op, field), nil
	default:
		slog.Error("convertFieldOpNormal not supported operator with value type", "op", op, "val type", reflect.TypeOf(value))
		errorStr := fmt.Sprintf("not supported operator value type,op: %s value type: %v", op, reflect.TypeOf(value))
		return "", errors.New(errorStr)
	}
}

func (fc *FilterConverter) convertOpInString(op string, field string, value string) string {
	return field + " in (" + value + ") "
}

func (fc *FilterConverter) convertFieldOpIn(op string, field string, value interface{}) (string, error) {
	//这里考虑对In操作的字段和值进行转换处理
	if fc.OperInConvert != nil {
		var err error
		field, value, err = fc.OperInConvert.Convert(op, field, value)
		if err != nil {
			return "", err
		}
	}

	switch value.(type) {
	case string:
		sVal := value.(string)
		return fc.convertOpInString(op, field, sVal), nil
	case []string:
		sliceVal := value.([]string)
		return fc.convertFieldValueStringArray(op, field, sliceVal), nil
	case []interface{}:
		sliceVal := value.([]interface{})
		return fc.convertFieldValueArray(op, field, sliceVal), nil
	default:
		slog.Error("convertFieldOpIn not supported operator with value type", "op", op, "val type", reflect.TypeOf(value))
		return "", errors.New("not supported operator value type,op: " + op + " value type: " + reflect.TypeOf(value).String())
	}
}

func (fc *FilterConverter) convertFieldValueMap(field string, value map[string]interface{}) (string, error) {
	var where string
	var str string
	var err error
	var index int = 0
	for key, value := range value {
		switch key {
		case Op_eq:
			str, err = fc.convertFieldOpNormal(" = ", field, value)
		case Op_ne:
			str, err = fc.convertFieldOpNormal(" <> ", field, value)
		case Op_gt:
			str, err = fc.convertFieldOpNormal(" > ", field, value)
		case Op_lt:
			str, err = fc.convertFieldOpNormal(" < ", field, value)
		case Op_gte:
			str, err = fc.convertFieldOpNormal(" >= ", field, value)
		case Op_lte:
			str, err = fc.convertFieldOpNormal(" <= ", field, value)
		case Op_in:
			str, err = fc.convertFieldOpIn(" in ", field, value)
		case Op_notIn:
			str, err = fc.convertFieldOpIn(" not in ", field, value)
		case Op_is:
			str, err = fc.convertFieldOpNormal(" is ", field, value)
		case Op_not:
			str, err = fc.convertFieldOpNormal(" is not ", field, value)
		case Op_like:
			str, err = fc.convertFieldOpNormal(" like ", field, value)
		case Op_or:
			str, err = fc.convertFieldArrayFilter("or", field, value)
		case Op_and:
			str, err = fc.convertFieldArrayFilter("and", field, value)
		default:
			//字段
			slog.Error("convertFieldValueMap not supported operator type", "operator type", key)
			return "", errors.New("not supported operator type " + key)
		}

		if err != nil {
			return "", err
		}

		if index == 0 {
			where = str
		} else {
			where = where + " and " + str
		}

		index++
	}
	return where, nil
}

func (fc *FilterConverter) convertFieldArrayFilter(logicOp string, field string, value interface{}) (string, error) {
	valueArray, ok := value.([]interface{})
	if !ok {
		slog.Error("convertFieldArrayFilter error,filter format error", "logicOp", logicOp, "value", value)
		return "", errors.New("convertFieldArrayFilter error, filter format error")
	}

	if len(valueArray) == 0 {
		slog.Error("convertFieldArrayFilter error,filter format error", "logicOp", logicOp, "value", value)
		return "", errors.New("convertFieldArrayFilter error, filter format error")
	}

	var where string = ""
	var str string
	var err error
	for _, v := range valueArray {
		//每个行应该是一个对象
		mVal, _ := v.(map[string]interface{})
		str, err = fc.convertFieldFilter(field, mVal)
		if err != nil {
			return "", err
		}
		where = where + " (" + str + ") " + logicOp
	}

	where = where[0 : len(where)-len(logicOp)]

	return where, nil
}

func (fc *FilterConverter) replaceApostrophe(str string) string {
	replacedStr := strings.ReplaceAll(str, "'", "''")
	return replacedStr
}
