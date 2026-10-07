package tasks_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

var (
	reflectValuesTestCases = []struct {
		name          string
		value         interface{}
		expectedType  string
		expectedValue interface{}
	}{
		// basic types
		{
			name:         "bool",
			value:        false,
			expectedType: "bool",
		},
		{
			name:          "int",
			value:         json.Number("123"),
			expectedType:  "int",
			expectedValue: int(123),
		},
		{
			name:          "int8",
			value:         json.Number("123"),
			expectedType:  "int8",
			expectedValue: int8(123),
		},
		{
			name:          "int16",
			value:         json.Number("123"),
			expectedType:  "int16",
			expectedValue: int16(123),
		},
		{
			name:          "int32",
			value:         json.Number("123"),
			expectedType:  "int32",
			expectedValue: int32(123),
		},
		{
			name:          "int64",
			value:         json.Number("185135722552891243"),
			expectedType:  "int64",
			expectedValue: int64(185135722552891243),
		},
		{
			name:          "uint",
			value:         json.Number("123"),
			expectedType:  "uint",
			expectedValue: uint(123),
		},
		{
			name:          "uint8",
			value:         json.Number("123"),
			expectedType:  "uint8",
			expectedValue: uint8(123),
		},
		{
			name:          "uint16",
			value:         json.Number("123"),
			expectedType:  "uint16",
			expectedValue: uint16(123),
		},
		{
			name:          "uint32",
			value:         json.Number("123"),
			expectedType:  "uint32",
			expectedValue: uint32(123),
		},
		{
			name:          "uint64",
			value:         json.Number("185135722552891243"),
			expectedType:  "uint64",
			expectedValue: uint64(185135722552891243),
		},
		{
			name:          "uint64",
			value:         json.Number("9223372036854775808"), // math.MaxInt64 + 1
			expectedType:  "uint64",
			expectedValue: uint64(9223372036854775808),
		},
		{
			name:          "float32",
			value:         json.Number("0.5"),
			expectedType:  "float32",
			expectedValue: float32(0.5),
		},
		{
			name:          "float64",
			value:         json.Number("0.5"),
			expectedType:  "float64",
			expectedValue: float64(0.5),
		},
		{
			name:          "string",
			value:         "123",
			expectedType:  "string",
			expectedValue: "123",
		},
		// slices
		{
			name:          "[]bool",
			value:         []interface{}{false, true},
			expectedType:  "[]bool",
			expectedValue: []bool{false, true},
		},
		{
			name:          "[]int",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]int",
			expectedValue: []int{1, 2},
		},
		{
			name:          "[]int8",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]int8",
			expectedValue: []int8{1, 2},
		},
		{
			name:          "[]int16",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]int16",
			expectedValue: []int16{1, 2},
		},
		{
			name:          "[]int32",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]int32",
			expectedValue: []int32{1, 2},
		},
		{
			name:          "[]int64",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]int64",
			expectedValue: []int64{1, 2},
		},
		{
			name:          "[]uint",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]uint",
			expectedValue: []uint{1, 2},
		},
		{
			name:          "[]uint8",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]uint8",
			expectedValue: []uint8{1, 2},
		},
		{
			name:          "[]uint16",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]uint16",
			expectedValue: []uint16{1, 2},
		},
		{
			name:          "[]uint32",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]uint32",
			expectedValue: []uint32{1, 2},
		},
		{
			name:          "[]uint64",
			value:         []interface{}{json.Number("1"), json.Number("2")},
			expectedType:  "[]uint64",
			expectedValue: []uint64{1, 2},
		},
		{
			name:          "[]float32",
			value:         []interface{}{json.Number("0.5"), json.Number("1.28")},
			expectedType:  "[]float32",
			expectedValue: []float32{0.5, 1.28},
		},
		{
			name:          "[]float64",
			value:         []interface{}{json.Number("0.5"), json.Number("1.28")},
			expectedType:  "[]float64",
			expectedValue: []float64{0.5, 1.28},
		},
		{
			name:          "[]string",
			value:         []interface{}{"foo", "bar"},
			expectedType:  "[]string",
			expectedValue: []string{"foo", "bar"},
		},
		// empty slices from NULL
		{
			name:          "[]bool",
			value:         nil,
			expectedType:  "[]bool",
			expectedValue: []bool{},
		},
		{
			name:          "[]int64",
			value:         nil,
			expectedType:  "[]int64",
			expectedValue: []int64{},
		},
		{
			name:          "[]uint64",
			value:         nil,
			expectedType:  "[]uint64",
			expectedValue: []uint64{},
		},
		{
			name:          "[]float64",
			value:         nil,
			expectedType:  "[]float64",
			expectedValue: []float64{},
		},
		{
			name:          "[]string",
			value:         nil,
			expectedType:  "[]string",
			expectedValue: []string{},
		},
	}
)

func TestReflectValue(t *testing.T) {
	t.Parallel()

	for _, tc := range reflectValuesTestCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			value, err := tasks.ReflectValue(tc.name, tc.value)
			if err != nil {
				t.Error(err)
			}
			if value.Type().String() != tc.expectedType {
				t.Errorf("type is %v, want %s", value.Type().String(), tc.expectedType)
			}
			if tc.expectedValue != nil {
				if !reflect.DeepEqual(value.Interface(), tc.expectedValue) {
					t.Errorf("value is %v, want %v", value.Interface(), tc.expectedValue)
				}
			}
		})
	}
}

func TestReflectValueUnsupportedType(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("notarealtype", "foo")
	assert.Error(t, err)
	assert.Equal(t, "notarealtype is not one of supported types", err.Error())

	unsupportedErr := tasks.NewErrUnsupportedType("notarealtype")
	assert.Equal(t, "notarealtype is not one of supported types", unsupportedErr.Error())
}

func TestReflectValuesUnsupportedType(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("[]notarealtype", "foo")
	assert.Error(t, err)
	assert.Equal(t, "[]notarealtype is not one of supported types", err.Error())
}

func TestReflectValueBoolConversionError(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("bool", "not-a-bool")
	assert.Error(t, err)
	assert.Equal(t, "not-a-bool is not bool", err.Error())
}

func TestReflectValueIntConversion(t *testing.T) {
	t.Parallel()

	value, err := tasks.ReflectValue("int", int64(42))
	assert.NoError(t, err)
	assert.Equal(t, int(42), value.Interface())

	_, err = tasks.ReflectValue("int", "not-an-int")
	assert.Error(t, err)
	assert.Equal(t, "not-an-int is not int", err.Error())

	_, err = tasks.ReflectValue("int", json.Number("not-a-number"))
	assert.Error(t, err)
}

func TestReflectValueUintConversion(t *testing.T) {
	t.Parallel()

	value, err := tasks.ReflectValue("uint", uint64(42))
	assert.NoError(t, err)
	assert.Equal(t, uint(42), value.Interface())

	value, err = tasks.ReflectValue("uint", uint8(7))
	assert.NoError(t, err)
	assert.Equal(t, uint(7), value.Interface())

	_, err = tasks.ReflectValue("uint", "not-a-uint")
	assert.Error(t, err)
	assert.Equal(t, "not-a-uint is not uint", err.Error())

	_, err = tasks.ReflectValue("uint", json.Number("not-a-number"))
	assert.Error(t, err)
}

func TestReflectValueFloatConversion(t *testing.T) {
	t.Parallel()

	value, err := tasks.ReflectValue("float64", float64(1.5))
	assert.NoError(t, err)
	assert.Equal(t, float64(1.5), value.Interface())

	_, err = tasks.ReflectValue("float64", "not-a-float")
	assert.Error(t, err)
	assert.Equal(t, "not-a-float is not float64", err.Error())

	_, err = tasks.ReflectValue("float64", json.Number("not-a-number"))
	assert.Error(t, err)
}

func TestReflectValueStringConversionError(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("string", 123)
	assert.Error(t, err)
	assert.Equal(t, "123 is not string", err.Error())
}

func TestReflectValuesBoolElementError(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("[]bool", []interface{}{true, "not-a-bool"})
	assert.Error(t, err)
}

func TestReflectValuesIntElementError(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("[]int", []interface{}{json.Number("1"), "not-an-int"})
	assert.Error(t, err)
}

func TestReflectValuesUintElementError(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("[]uint", []interface{}{json.Number("1"), "not-a-uint"})
	assert.Error(t, err)
}

func TestReflectValuesUintElementDirectTypes(t *testing.T) {
	t.Parallel()

	value, err := tasks.ReflectValue("[]uint64", []interface{}{uint64(1), uint64(2)})
	assert.NoError(t, err)
	assert.Equal(t, []uint64{1, 2}, value.Interface())
}

func TestReflectValuesFloatElementError(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("[]float64", []interface{}{json.Number("1.1"), "not-a-float"})
	assert.Error(t, err)
}

func TestReflectValuesStringElementError(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("[]string", []interface{}{"a", 5})
	assert.Error(t, err)
}

func TestReflectValuesByteSliceFromBase64String(t *testing.T) {
	t.Parallel()

	encoded := base64.StdEncoding.EncodeToString([]byte("hello"))

	value, err := tasks.ReflectValue("[]byte", encoded)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), value.Interface())
}

func TestReflectValuesByteSliceInvalidBase64String(t *testing.T) {
	t.Parallel()

	_, err := tasks.ReflectValue("[]byte", "not-valid-base64!!!")
	assert.Error(t, err)
}

func TestIsContextType(t *testing.T) {
	t.Parallel()

	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	assert.True(t, tasks.IsContextType(ctxType))

	notCtxType := reflect.TypeOf("string")
	assert.False(t, tasks.IsContextType(notCtxType))
}
