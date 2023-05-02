package nse

import (
	"errors"
	"fmt"
	"math"

	"github.com/golang/glog"
)

func getStrField(
	records map[string]interface{},
	field string) (string, error) {

	fieldInt, ok := records[field]
	if !ok {
		msg := fmt.Sprintf("Parsing OC records failed. Field %s not found.",
			field)
		glog.Error(msg)
		return "", errors.New(msg)
	}
	value, ok := fieldInt.(string)
	if !ok {
		msg := fmt.Sprintf("Parsing OC records failed."+
			"Field %s is not of string type.", field)
		glog.Error(msg)
		return "", errors.New(msg)
	}
	return value, nil
}

func getFloat64Field(
	records map[string]interface{},
	field string) (float64, error) {

	fieldValue, ok := records[field]
	if !ok {
		msg := fmt.Sprintf("Parsing records failed. Field %s not found.", field)
		glog.Error(msg)
		return 0, errors.New(msg)
	}

	value, ok := fieldValue.(float64)
	if !ok {
		msg := fmt.Sprintf("Parsing records failed."+
			"Field %s is not of float64 type.", field)
		glog.Error(msg)
		return 0, errors.New(msg)
	}

	return value, nil
}

func getIntField(
	records map[string]interface{},
	field string) (int, error) {

	fieldValue, ok := records[field]
	if !ok {
		msg := fmt.Sprintf("Parsing records failed. Field %s not found.", field)
		glog.Error(msg)
		return 0, errors.New(msg)
	}

	value, ok := fieldValue.(int)
	if !ok {
		msg := fmt.Sprintf("Parsing field=%s failed. "+
			"Expected int type found %T type.", field, fieldValue)
		glog.Error(msg, value)
		return 0, errors.New(msg)
	}

	return value, nil
}

func getArrayField(
	records map[string]interface{},
	field string) ([]interface{}, error) {

	fieldInt, ok := records[field]
	if !ok {
		msg := fmt.Sprintf("Parsing OC records failed. Field %s not found.",
			field)
		return []interface{}{}, errors.New(msg)
	}
	value, ok := fieldInt.([]interface{})
	if !ok {
		msg := fmt.Sprintf("Parsing OC records failed."+
			"Field %s is not of array type.", field)
		return []interface{}{}, errors.New(msg)
	}
	return value, nil
}

func getStringToInterfaceMap(
	records map[string]interface{},
	field string) (result map[string]interface{}, err error) {

	fieldInt, ok := records[field]
	if !ok {
		msg := fmt.Sprintf("Field %s not found in the records.", field)
		glog.Error(msg)
		return result, errors.New(msg)
	}
	result, ok = fieldInt.(map[string]interface{})
	if !ok {
		msg := fmt.Sprintf("Unexpected field %s type", field)
		glog.Error(msg)
		return result, errors.New(msg)
	}
	return result, nil
}

func convertToStringSlice(data []interface{}) []string {
	result := make([]string, len(data))
	for i, v := range data {
		if str, ok := v.(string); ok {
			result[i] = str
		} else {
			// Handle the case where the element is not a string
			// You can choose to skip, ignore, or perform some other action
			result[i] = ""
			glog.Info(fmt.Sprintf("Value %v is not string.", v))
		}
	}
	return result
}

func convertToIntSlice(data []interface{}) []int32 {
	result := make([]int32, len(data))
	for i, v := range data {
		if str, ok := v.(int32); ok {
			result[i] = str
		} else {
			// Handle the case where the element is not a string
			// You can choose to skip, ignore, or perform some other action
			result[i] = 0
			glog.Info(fmt.Sprintf("Value %v is not integer.", v))
		}
	}
	return result
}

func roundToStep(num float64, step int32) int32 {
	// round the input number to the nearest integer
	rounded := int32(math.Round(num))
	// calculate the remainder when divided by 50
	remainder := rounded % step
	// calculate the difference from the nearest multiple of 50
	diff := step - remainder
	// adjust the rounded number based on the difference
	if remainder <= (step / 2) {
		return rounded - remainder
	} else {
		return rounded + diff
	}
}
