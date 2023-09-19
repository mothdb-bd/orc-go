package block

import (
	"math"
	"reflect"
	"strconv"
)

// BigInt structure
type BigInt struct {
	// array used to represent the value of a BigInt
	// each index represents a digit in a number
	// uint8 is used since no digits will exceed 9,
	// and the range of uint8 is from 0 to 255,
	// the int is unsigned since a single digit
	// will never be negative
	value []uint8

	// boolean used to represent whether a BigInt is positive or not
	isPositive bool
}

var TEN BigInt = *NewBigIntString("10")

// redirects to the NewBigIntString function with the correct input
func NewBigInt(i ...interface{}) *BigInt {
	// if there is an input passed, check it's type
	// input is an array due to the potention of 0 or 1 input passed
	if len(i) > 0 {
		// switches through the possibe input types
		switch reflect.TypeOf(i[0]) {

		// if input is a string, redirect it as is
		case reflect.TypeOf("string"):
			{
				return NewBigIntString(i[0].(string))
			}
		// if input is an int, convert it to a int64, then to a string, then redirect it
		case reflect.TypeOf(int(0)):
			{
				return NewBigIntString(strconv.FormatInt(int64(i[0].(int)), 10))
			}
		// if input is an int8, convert it to an int64, then to a string, then redirect it
		case reflect.TypeOf(int8(0)):
			{
				return NewBigIntString(strconv.FormatInt(int64(i[0].(int8)), 10))
			}
		// if input is an int16, convert it to an int64, then to a string, then redirect it
		case reflect.TypeOf(int16(0)):
			{
				return NewBigIntString(strconv.FormatInt(int64(i[0].(int16)), 10))
			}
		// if input is an int32, convert it to an int64, then to a string, then redirect it
		case reflect.TypeOf(int32(0)):
			{
				return NewBigIntString(strconv.FormatInt(int64(i[0].(int32)), 10))
			}
		// if input is an int64, convert it to a string, then redirect it
		case reflect.TypeOf(int64(0)):
			{
				return NewBigIntString(strconv.FormatInt(i[0].(int64), 10))
			}
		// if input is an uint64, convert it to a string, then redirect it
		case reflect.TypeOf(uint(0)):
			{
				return NewBigIntString(strconv.FormatUint(uint64(i[0].(uint)), 10))
			}
		// if input is an uint8, convert it to an uint64, then to a string, then redirect it
		case reflect.TypeOf(uint8(0)):
			{
				return NewBigIntString(strconv.FormatUint(uint64(i[0].(uint8)), 10))
			}
		// if input is an uint16, convert it to an uint64, then to a string, then redirect it
		case reflect.TypeOf(uint16(0)):
			{
				return NewBigIntString(strconv.FormatUint(uint64(i[0].(uint16)), 10))
			}
		// if input is an uint32, convert it to an uint64, then to a string, then redirect it
		case reflect.TypeOf(uint32(0)):
			{
				return NewBigIntString(strconv.FormatUint(uint64(i[0].(uint32)), 10))
			}
		// if input is an uint64, convert it to a string, then redirect it
		case reflect.TypeOf(uint64(0)):
			{
				return NewBigIntString(strconv.FormatUint(i[0].(uint64), 10))
			}

		}
	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with an empty string as input
	return NewBigIntString("")
}

// returns a new BigInt data type pointer
// uses value_str in order to determine it's value
func NewBigIntString(value_str string) *BigInt {
	// creates a new BigInt type to return
	new_big_int := new(BigInt)
	// if the string is empty, set the value to 0
	if len(value_str) == 0 {
		new_big_int.SetValue("0")
		// if the string is not empty, set the value to the string's representation
	} else {
		new_big_int.SetValue(value_str)
	}
	// returns the new BigInt data type we created
	return new_big_int
}

// returns an array of BigInt data type pointers
// big_int_array is the unsorted array
func Sort(big_int_array []*BigInt) []*BigInt {

	// creates array to return
	sorted_array := []*BigInt{}

	// appends the smallest big int to sorted until only 1 is left
	for len(big_int_array) > 1 {
		// used to keep track of the smallest BigInt's value and index
		// start values represent the first index
		smallest_big_int := big_int_array[0].GetCopy()
		smallest_big_int_index := uint64(0)
		// goes through each index and compares it's value with the current
		// smallest value, starts at 1 since the default values are equal to index 0's values
		for index := uint64(1); index < uint64(len(big_int_array)); index++ {

			// checks if current big int is less than or equal to smallest big int,
			// and if so, updates our smallest BigInt tracking variables
			if big_int_array[index].IsLessThanOrEqualTo(smallest_big_int) {
				smallest_big_int = big_int_array[index].GetCopy()
				smallest_big_int_index = index
			}
		}

		// appends the smallest BigInt to the incomplete sorted array
		sorted_array = append(sorted_array, smallest_big_int)

		// removes the smallest value from the unsorted array
		// reassigns the array to be all the values up to, but not including
		// the smallest BigInt, then appends all BigInts after the smallest
		big_int_array_temp := big_int_array
		big_int_array = big_int_array[:smallest_big_int_index]
		for _, bi := range big_int_array_temp[smallest_big_int_index+1:] {
			big_int_array = append(big_int_array, bi)
		}
	}

	// appends the last BigInt to the now soon-to-be completed sorted array
	sorted_array = append(sorted_array, big_int_array[0])

	// returns the completed sorted array
	return sorted_array
}

// redirects to SetValueString with the correct input
func (big_int *BigInt) SetValue(i interface{}) bool {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a string, redirect it as is
	case reflect.TypeOf("string"):
		{
			return big_int.SetValueString(i.(string))
		}
	// if input is an int, convert it to an int64, then to a string, then redirect it
	case reflect.TypeOf(0):
		{
			return big_int.SetValueString(strconv.FormatInt(int64(i.(int)), 10))
		}
	// if input is an int8, convert it to an int64, then to a string, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.SetValueString(strconv.FormatInt(int64(i.(int8)), 10))
		}
	// if input is an int16, convert it to an int64, then to a string, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.SetValueString(strconv.FormatInt(int64(i.(int16)), 10))
		}
	// if input is an int32, convert it to an int64, then to a string, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.SetValueString(strconv.FormatInt(int64(i.(int32)), 10))
		}
	// if input is an int64, convert it to a string, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.SetValueString(strconv.FormatInt(i.(int64), 10))
		}
	// if input is an uint, convert it to an uint64, then to a string, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.SetValueString(strconv.FormatUint(uint64(i.(uint)), 10))
		}
	// if input is an uint8, convert it to an uint64, then to a string, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.SetValueString(strconv.FormatUint(uint64(i.(uint8)), 10))
		}
	// if input is an uint16, convert it to an uint64, then to a string, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.SetValueString(strconv.FormatUint(uint64(i.(uint16)), 10))
		}
	// if input is an uint32, convert it to an uint64, then to a string, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.SetValueString(strconv.FormatUint(uint64(i.(uint32)), 10))
		}
	// if input is an uint, convert it to a string, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.SetValueString(strconv.FormatUint(i.(uint64), 10))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with an empty string as input
	return big_int.SetValueString("")
}

// changes the value of a BigInt, then returns whether or not we succeeded
func (big_int *BigInt) SetValueString(value_str string) bool {
	// creates temp value in case of false return,
	// then clears the current value
	value_temp := big_int.value
	big_int.value = []uint8{}

	// makes sure an empty string was not passed
	// if one was, resets the value and returns false
	if len(value_str) < 1 {
		big_int.value = value_temp
		return false
	}

	// makes sure if the number is negative, at least two values are passed
	// if not, resets the value and returns false
	if string(value_str[0]) == "-" && !(len(value_str) >= 2) {
		big_int.value = value_temp
		return false
	}

	// removes the leading "+" if passed, then verifies length is still greater than 1
	// if not, resets the value and returns false
	if string(value_str[0]) == "+" {
		if !(len(value_str) >= 2) {
			big_int.value = value_temp
			return false
		} else {
			value_str = value_str[1:]
		}
	}

	// removes any commas within the input string passed
	// goes through each index and checks if it is a ",",
	// if it is, reassigns the string to be every index
	// up to, but not including the comma, then appends
	// every index after the comma so long as that index is not
	// also a string, if an index is removed, lowers the index variable by 1
	// in order to prevent skipping over the next index
	for index := 0; index < len(value_str); index++ {
		if string(value_str[index]) == "," {
			value_str_temp := value_str
			value_str = value_str[:index]
			value_str_to_append := value_str_temp[index+1:]
			for _, c := range value_str_to_append {
				if string(c) != "," {
					value_str += string(c)
				}
			}
			index--
		}
	}

	// makes sure all characters passed are numerical or a negative sign
	// if not, resets the value and returns false
	for _, c := range []byte(value_str) {
		var contains bool = false
		for _, n := range []byte("-0123456789") {
			if c == n {
				contains = true
			}
		}
		if !contains {
			big_int.value = value_temp
			return false
		}
	}

	// if the value input string is "-0", resets it "0"
	if value_str == "-0" {
		value_str = "0"
	}

	// sets whether the BigInt is positive or negative
	if string(value_str[0]) == "-" {
		big_int.isPositive = false
		value_str = value_str[1:]
	} else {
		big_int.isPositive = true
	}

	// assigns the BigInt's value variable by going through
	// each index in the value string, converting it to an uint8,
	// then appending it to the BigInt's value variable
	// uint8 is used since no digits will exceed 9, and the range of uint8 is
	// from 0 to 255, the int is unsigned since a single digit will never
	// be negative
	for index := 0; index < len(value_str); index++ {
		digit, _ := strconv.ParseInt(string(value_str[index]), 10, 8)
		big_int.value = append(big_int.value, uint8(digit))
	}

	// if completed with no false returns, aka failures, returns true
	return true
}

// returns a copy of the BigInt's value array
func (big_int *BigInt) GetValueArray() []uint8 {
	return big_int.value
}

// returns an int representation of the BigInt
func (big_int *BigInt) GetInt() int {
	// if the BigInt's value exceeds the range of int,
	// returns the default value, 0
	if big_int.IsGreaterThan(math.MaxInt) {
		return 0
	}
	// returns int32 if that's the maximum value for an int;
	// this is the case for 32-bit systems
	if math.MaxInt == math.MaxInt32 {
		return int(big_int.GetInt32())
	}
	// returns int64
	return int(big_int.GetInt64())
}

// returns an int8 representation of the BigInt
func (big_int *BigInt) GetInt8() int8 {
	// if the BigInt's value exceeds the range of int8,
	// returns the default value, 0
	if big_int.IsGreaterThan(math.MaxInt8) {
		return 0
	}
	// returns int8
	return int8(big_int.GetInt32())
}

// returns an int16 representation of the BigInt
func (big_int *BigInt) GetInt16() int16 {
	// if the BigInt's value exceeds the range of int16,
	// returns the default value, 0
	if big_int.IsGreaterThan(math.MaxInt16) {
		return 0
	}
	// returns int16
	return int16(big_int.GetInt32())
}

// returns an int32 representation of the BigInt
func (big_int *BigInt) GetInt32() int32 {
	// if the BigInt's value exceeds the range of int32,
	// returns the default value, 0
	if big_int.IsGreaterThan(math.MaxInt32) {
		return 0
	}
	// sum variables used to track the sum of the digits
	var sum int32 = 0
	// goes through each digit and adds it value
	// multiplied by 10 to the power of it's index
	// to the sum variable
	for index := 0; index < len(big_int.value); index++ {
		sum += int32(math.Pow10(len(big_int.value)-index-1)) * int32(big_int.value[index])
	}
	// if the BigInt is negative,
	// multiply the sum by -1
	if !big_int.isPositive {
		sum *= -1
	}
	// returns the sum
	return sum
}

// returns an int64 representation of the BigInt
func (big_int *BigInt) GetInt64() int64 {
	// if the BigInt's value exceeds the range of int64,
	// returns the default value, 0
	if big_int.IsGreaterThan(math.MaxInt64) {
		return 0
	}
	// sum variables used to track the sum of the digits
	var sum int64 = 0
	// goes through each digit and adds it value
	// multiplied by 10 to the power of it's index
	// to the sum variable
	for index := 0; index < len(big_int.value); index++ {
		sum += int64(math.Pow(10, float64(len(big_int.value)-index-1))) * int64(big_int.value[index])
	}
	// if the BigInt is negative,
	// multiply the sum by -1
	if !big_int.isPositive {
		sum *= -1
	}
	// returns the sum
	return sum
}

// returns an uint representation of the BigInt
func (big_int *BigInt) GetUnsignedInt() uint {
	// if the BigInt's value exceeds the range of int,
	// returns the default value, 0
	// note: math.MaxUint is casted as an uint because "without an explicit type, by default, integer untyped constants will get an int type, which can't hold math.MaxUint"
	if big_int.IsGreaterThan(uint(math.MaxUint)) {
		return 0
	}
	// returns int32 if that's the maximum value for an int;
	// this is the case for 32-bit systems
	if math.MaxInt == math.MaxUint32 {
		return uint(big_int.GetUnsignedInt32())
	}
	// returns int64
	return uint(big_int.GetUnsignedInt32())
}

// returns an uint8 representation of the BigInt
func (big_int *BigInt) GetUnsignedInt8() uint8 {
	// if the BigInt's value exceeds the range of uint8,
	// returns the default value, 0
	if big_int.IsGreaterThan(math.MaxUint8) {
		return 0
	}
	return uint8(big_int.GetUnsignedInt32())
}

// returns an uint16 representation of the BigInt
func (big_int *BigInt) GetUnsignedInt16() uint16 {
	// if the BigInt's value exceeds the range of uint16,
	// returns the default value, 0
	if big_int.IsGreaterThan(math.MaxUint16) {
		return 0
	}
	return uint16(big_int.GetUnsignedInt32())
}

// returns an uint32 representation of the BigInt
func (big_int *BigInt) GetUnsignedInt32() uint32 {
	// if the BigInt's value exceeds the range of uint32,
	// returns the default value, 0
	if big_int.IsGreaterThan(math.MaxUint32) {
		return 0
	}
	// sum variables used to track the sum of the digits
	var sum uint32 = 0
	// goes through each digit and adds it value
	// multiplied by 10 to the power of it's index
	// to the sum variable
	for index := 0; index < len(big_int.value); index++ {
		sum += uint32(math.Pow10(len(big_int.value)-index-1)) * uint32(big_int.value[index])
	}

	// returns the sum
	return sum
}

// returns an int64 representation of the BigInt
func (big_int *BigInt) GetUnsignedInt64() uint64 {
	// if the BigInt's value exceeds the range of int64,
	// returns the default value, 0
	// note: math.MaxUint64 is casted as an uint64 because "without an explicit type, by default, integer untyped constants will get an int type, which can't hold math.MaxUint64"
	if big_int.IsGreaterThan(uint64(math.MaxUint64)) {
		return 0
	}
	// sum variables used to track the sum of the digits
	var sum uint64 = 0
	// goes through each digit and adds it value
	// multiplied by 10 to the power of it's index
	// to the sum variable
	for index := 0; index < len(big_int.value); index++ {
		sum += uint64(math.Pow(10, float64(len(big_int.value)-index-1))) * uint64(big_int.value[index])
	}
	// returns the sum
	return sum
}

// returns a BigInt that is identical
func (big_int *BigInt) GetCopy() *BigInt {
	new_big_int := NewBigInt(big_int.String())
	return new_big_int
}

func (big_int *BigInt) StringNum() string {

	// holds the string variable
	var to_str = ""

	// appends the string to each digit
	for index := len(big_int.value) - 1; index >= 0; index-- {
		to_str = strconv.FormatInt(int64(big_int.value[index]), 10) + to_str
	}

	// adds the negative sign if applicable
	if !big_int.IsPositive() {
		to_str = "-" + to_str
	}

	// returns the string
	return to_str
}

// returns the string representation of a BigInt
func (big_int *BigInt) String() string {

	// holds the string variable
	var to_str = ""

	// appends the string to each digit
	// every 3 digits, add a comma
	var comma_cnt int = 0
	for index := len(big_int.value) - 1; index >= 0; index-- {
		comma_cnt++
		to_str = strconv.FormatInt(int64(big_int.value[index]), 10) + to_str
		if comma_cnt == 3 && index != 0 {
			to_str = "," + to_str
			comma_cnt = 0
		}
	}

	// adds the negative sign if applicable
	if !big_int.IsPositive() {
		to_str = "-" + to_str
	}

	// returns the string
	return to_str
}

// returns a copy of BigInt's isPositive boolean
func (big_int *BigInt) IsPositive() bool {
	return big_int.isPositive
}

// returns a BigInt identical to the input BigInt,
// except for the fact that the isPositive variable
// is set to the opposite
func (big_int *BigInt) GetSwitchedSign() *BigInt {
	copy_big_int := big_int.GetCopy()
	copy_big_int.isPositive = !copy_big_int.isPositive
	return copy_big_int
}

// redirects to IsEqualToBigInt with the correct input
func (big_int *BigInt) IsEqualTo(i interface{}) bool {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.IsEqualToBigInt(i.(*BigInt))
		}
	// if input is a string, convert it to a BigInt, then redirect it
	case reflect.TypeOf("string"):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.IsEqualToBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.IsEqualToBigInt(NewBigInt())
}

// returns a boolean representing whether two BigInts are of equal value
func (big_int *BigInt) IsEqualToBigInt(other_big_int *BigInt) bool {

	// makes sure both have the same sign
	if big_int.IsPositive() != other_big_int.IsPositive() {
		return false
	}

	// makes sure the length of both values is equal
	if len(big_int.value) != len(other_big_int.value) {
		return false
	}

	// checks if each digit is equal
	for index := 0; index < len(big_int.value); index++ {
		if big_int.value[index] != other_big_int.value[index] {
			return false
		}
	}

	// if all tests have been passed, returns true
	return true
}

// redirects to IsNotEqualToBigInt with the correct input
func (big_int *BigInt) IsNotEqualTo(i interface{}) bool {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.IsNotEqualToBigInt(i.(*BigInt))
		}
	// if input is a string, convert it to a BigInt, then redirect it
	case reflect.TypeOf("string"):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.IsNotEqualToBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.IsNotEqualToBigInt(NewBigInt())
}

// returns a boolean representing whether two BigInts are not of equal value
func (big_int *BigInt) IsNotEqualToBigInt(other_big_int *BigInt) bool {
	return !big_int.IsEqualTo(other_big_int)
}

// redirects to IsGreaterThanBigInt with the correct input
func (big_int *BigInt) IsGreaterThan(i interface{}) bool {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.IsGreaterThanBigInt(i.(*BigInt))
		}
	// if input is a string, convert it to a BigInt, then redirect it
	case reflect.TypeOf("string"):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.IsGreaterThanBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.IsGreaterThanBigInt(NewBigInt())
}

// returns a boolean representing whether one BigInt is greater than another
func (big_int *BigInt) IsGreaterThanBigInt(other_big_int *BigInt) bool {

	// if big int is negative and the other is positive, return false
	if big_int.IsPositive() == false && other_big_int.IsPositive() == true {
		return false
	}

	// if big int has less digits, is is smaller, return false
	if len(big_int.value) < len(other_big_int.value) {
		return false
	}

	// if big int has more digits, it is bigger, return true
	if len(big_int.value) > len(other_big_int.value) {
		return true
	}

	// if they are the same length, makes sure no digits in other are bigger
	for index := 0; index < len(big_int.value); index++ {
		if big_int.value[index] > other_big_int.value[index] {
			return true
		} else if big_int.value[index] < other_big_int.value[index] {
			return false
		}
	}

	// if nothing has been returned so far, they are equal, return false
	return false
}

// redirects to IsGreaterThanOrEqualToBigInt with the correct input
func (big_int *BigInt) IsGreaterThanOrEqualTo(i interface{}) bool {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(i.(*BigInt))
		}
	// if input is a string, convert it to a BigInt, then redirect it
	case reflect.TypeOf("string"):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.IsGreaterThanOrEqualToBigInt(NewBigInt())
}

// returns a boolean representing whether one BigInt is greater than or equal to another
func (big_int *BigInt) IsGreaterThanOrEqualToBigInt(other_big_int *BigInt) bool {
	return big_int.IsEqualTo(other_big_int) || big_int.IsGreaterThan(other_big_int)
}

// redirects to IsLessThanBigInt with the correct input
func (big_int *BigInt) IsLessThan(i interface{}) bool {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.IsLessThanBigInt(i.(*BigInt))
		}
	// if input is a string, convert it to a BigInt, then redirect it
	case reflect.TypeOf("string"):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.IsLessThanBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.IsLessThanBigInt(NewBigInt())
}

// returns a boolean representing whether one BigInt is less than another
func (big_int *BigInt) IsLessThanBigInt(other_big_int *BigInt) bool {
	return !big_int.IsGreaterThanOrEqualTo(other_big_int)
}

// redirects to IsLessThanOrEqualToBigInt with the correct input
func (big_int *BigInt) IsLessThanOrEqualTo(i interface{}) bool {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.IsLessThanOrEqualToBigInt(i.(*BigInt))
		}
	// if input is a string, convert it to a BigInt, then redirect it
	case reflect.TypeOf("string"):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.IsLessThanOrEqualToBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.IsLessThanOrEqualToBigInt(NewBigInt())
}

// returns a boolean representing whether one BigInt is less than or equal to another
func (big_int *BigInt) IsLessThanOrEqualToBigInt(other_big_int *BigInt) bool {
	return big_int.IsEqualTo(other_big_int) || big_int.IsLessThan(other_big_int)
}

// returns a BigInt identical to the input BigInt,
// except for the fact that the isPositive variable
// is always set to true
func (big_int *BigInt) Abs() *BigInt {
	abs := big_int.GetCopy()
	abs.isPositive = true
	return abs
}

// redirects to AddBigInt with the correct input
func (big_int *BigInt) Add(i interface{}) *BigInt {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt pointer, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.AddBigInt(i.(*BigInt))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.AddBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.Add(NewBigInt())
}

// returns a BigInt that represents the sum of two BigInts
func (big_int *BigInt) AddBigInt(other_big_int *BigInt) *BigInt {

	// if positive plus a negative, performs the proper redirect
	if big_int.IsPositive() == true && other_big_int.IsPositive() == false {
		return big_int.Subtract(other_big_int.GetSwitchedSign())
	}

	// if negative plus a positive, performs the proper redirect
	if big_int.IsPositive() == false && other_big_int.IsPositive() == true {
		return other_big_int.Subtract(big_int.GetSwitchedSign())
	}

	// if negative plus a negative, performs the proper redirect
	if big_int.IsPositive() == false && other_big_int.IsPositive() == false {
		return big_int.GetSwitchedSign().Add(other_big_int.GetSwitchedSign()).GetSwitchedSign()
	}

	// adds using built-in math if capable
	// checks if each is less than or equal to half
	// of the max value for an uint, this guarantees the sum
	// will never be greater than the max value of uint
	var max_value uint = math.MaxUint / 2
	if big_int.IsLessThanOrEqualTo(max_value) {
		if other_big_int.IsLessThanOrEqualTo(max_value) {
			return NewBigInt(big_int.GetUnsignedInt() + other_big_int.GetUnsignedInt())
		}
	}

	// gets the maximum length of the sum's value array
	// it will be the length of the longest value array
	// plus 1 to account for carrying
	max_length := 1 + int(math.Max(float64(len(big_int.value)), float64(len(other_big_int.value))))

	// fills biv (short for big_int_values) with the required amount leading zeros
	var biv []uint8
	for x := 0; x < max_length-len(big_int.value); x++ {
		biv = append(biv, 0)
	}

	// appends biv with big_int's array values
	for _, d := range big_int.value {
		biv = append(biv, d)
	}

	// fills obiv (short for other_big_int_values) with the required amount leading zeros
	var obiv []uint8
	for x := 0; x < max_length-len(other_big_int.value); x++ {
		obiv = append(obiv, 0)
	}

	// appends biv with big_int's array values
	for _, d := range other_big_int.value {
		biv = append(obiv, d)
	}

	// creates the array to hold the sum's value, will be filled with zeros by default
	var sum_value []uint8
	for index := 0; index < max_length; index++ {
		sum_value = append(sum_value, 0)
	}

	// adds the two values
	var carry uint8 = 0
	for index := len(biv) - 1; index >= 0; index-- {
		var sum uint8 = biv[index] + obiv[index] + carry
		if sum >= 10 {
			carry = uint8(sum / 10)
			sum = sum - 10
		} else {
			carry = 0
		}
		sum_value[index] = sum
	}

	// creates a string representation of the sum's value array
	var sum_value_str string = ""
	for _, d := range sum_value {
		sum_value_str += strconv.FormatInt(int64(d), 10)
	}

	// removes leading 0 if it is there and not the only digit
	if sum_value_str[0] == []byte("0")[0] && len(sum_value_str) > 1 {
		sum_value_str = string(sum_value_str[1:])
	}

	// creates the big int we will return
	sum_bigint := NewBigInt(sum_value_str)

	// returns the new big int
	return sum_bigint
}

// redirects to SubtractBigInt with the correct input
func (big_int *BigInt) Subtract(i interface{}) *BigInt {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt pointer, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.SubtractBigInt(i.(*BigInt))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.SubtractBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.SubtractBigInt(NewBigInt())
}

// returns a BigInt that represents the difference of two BigInts
func (big_int *BigInt) SubtractBigInt(other_big_int *BigInt) *BigInt {

	// if positive subtracting a negative, performs the proper redirect
	if big_int.IsPositive() == true && other_big_int.IsPositive() == false {
		return big_int.Add(other_big_int.GetSwitchedSign())
	}

	// if negative subtracting a positive, performs the proper redirect
	if big_int.IsPositive() == false && other_big_int.IsPositive() == true {
		return big_int.GetSwitchedSign().Add(other_big_int).GetSwitchedSign()
	}

	// // if negative subtracting a negative, performs the proper redirect
	if big_int.IsPositive() == false && other_big_int.IsPositive() == false {
		return big_int.Add(other_big_int.GetSwitchedSign())
	}

	// if big_int is less than other_big_int, performs the proper redirect
	if big_int.IsLessThan(other_big_int) {
		return other_big_int.Subtract(big_int).GetSwitchedSign()
	}

	// subtracts using built-in math if capable
	// checks if each is less than or equal to
	// the max value for an uint
	var max_value uint = math.MaxUint
	if big_int.IsLessThanOrEqualTo(max_value) {
		if other_big_int.IsLessThanOrEqualTo(max_value) {
			return NewBigInt(big_int.GetUnsignedInt() - other_big_int.GetUnsignedInt())
		}
	}

	// gets the maximum length of the differences's value array
	// it will be the length of the longest value array
	max_length := int(math.Max(float64(len(big_int.value)), float64(len(other_big_int.value))))

	// fills biv (short for big_int_values) with the required amount leading zeros
	var biv []uint8
	for x := 0; x < max_length-len(big_int.value); x++ {
		biv = append(biv, 0)
	}

	// appends biv with big_int's array values
	for _, d := range big_int.value {
		biv = append(biv, d)
	}

	// fills obiv (short for other_big_int_values) with the required amount leading zeros
	var obiv []uint8
	for x := 0; x < max_length-len(other_big_int.value); x++ {
		obiv = append(obiv, 0)
	}

	// appends biv with big_int's array values
	for _, d := range other_big_int.value {
		biv = append(obiv, d)
	}

	// creates the array to hold the differences's value, will be filled with zeros by default
	var diff_value []uint8
	for index := 0; index < max_length; index++ {
		diff_value = append(diff_value, 0)
	}

	// subtracts the two values
	for index := len(biv) - 1; index > 0; index-- {
		if biv[index] < obiv[index] {
			biv[index-1] -= 1
			biv[index] += 10
			diff_value[index] = biv[index] - obiv[index]
		} else {
			diff_value[index] = biv[index] - obiv[index]
		}
	}

	// creates a string representation of the value
	var diff_value_str string = ""
	for _, d := range diff_value {
		diff_value_str += strconv.FormatInt(int64(d), 10)
	}

	// removes leading 0s if it is there and not the only character
	for diff_value_str[0] == []byte("0")[0] && len(diff_value_str) > 1 {
		diff_value_str = string(diff_value_str[1:])
	}

	// creates the big int we will return
	diff_bigint := NewBigInt(diff_value_str)

	// returns the new big int
	return diff_bigint
}

// redirects to MultiplyBigInt with the correct input
func (big_int *BigInt) Multiply(i interface{}) *BigInt {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt pointer, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.MultiplyBigInt(i.(*BigInt))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.MultiplyBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.MultiplyBigInt(NewBigInt())
}

// returns a BigInt that represents the product of two BigInts
func (big_int *BigInt) MultiplyBigInt(other_big_int *BigInt) *BigInt {

	// multiplies using built-in math if capable
	// checks if each is less than or equal to
	// the square root of the max value for an uint,
	// this guarantees the product will never be greater
	// than the max value for uint or int i
	// at least one BigInt is negative
	if big_int.IsPositive() && other_big_int.IsPositive() {
		var max_value uint = uint(math.Sqrt(math.MaxUint))
		if big_int.IsLessThanOrEqualTo(max_value) && other_big_int.IsLessThanOrEqualTo(max_value) {
			return NewBigInt(big_int.GetUnsignedInt() * other_big_int.GetUnsignedInt())
		}
	} else {
		var max_value int = int(math.Sqrt(math.MaxInt))
		if big_int.IsLessThanOrEqualTo(max_value) && other_big_int.IsLessThanOrEqualTo(max_value) {
			return NewBigInt(big_int.GetInt() * other_big_int.GetInt())
		}
	}

	// counts number of trailing zeros in big int
	var value_arr_big_int []uint8 = big_int.value
	var trailing_zero_cnt_big_int int = 0
	for index := len(value_arr_big_int) - 1; index >= 0; index-- {
		if value_arr_big_int[index] == uint8(0) {
			trailing_zero_cnt_big_int++
		} else {
			break
		}
	}

	// counts number of trailing zeros in other big int
	var value_arr_other_big_int []uint8 = other_big_int.value
	var trailing_zero_cnt_other_big_int int = 0
	for index := len(value_arr_other_big_int) - 1; index >= 0; index-- {
		if value_arr_other_big_int[index] == uint8(0) {
			trailing_zero_cnt_other_big_int++
		} else {
			break
		}
	}

	// creates the big int to return
	product := NewBigInt()

	// if there both BigInts have trailing zeros, gets muliplication without them then adds them back to the product and returns said product
	if trailing_zero_cnt_big_int > 0 && trailing_zero_cnt_other_big_int > 0 {

		var value_str string = ""
		for index := 0; index < len(value_arr_big_int); index++ {
			value_str += strconv.FormatInt(int64(value_arr_big_int[index]), 10)
		}
		big_int_no_zeros := big_int.GetCopy()
		big_int_no_zeros.SetValue(value_str[:len(value_str)-trailing_zero_cnt_big_int])

		value_str = ""
		for index := 0; index < len(value_arr_other_big_int); index++ {
			value_str += strconv.FormatInt(int64(value_arr_other_big_int[index]), 10)
		}
		other_big_int_no_zeros := other_big_int.GetCopy()
		other_big_int_no_zeros.SetValue(value_str[:len(value_str)-trailing_zero_cnt_other_big_int])

		product = big_int_no_zeros.Multiply(other_big_int_no_zeros)
		var total_zero_count int = trailing_zero_cnt_big_int + trailing_zero_cnt_other_big_int
		for z := 0; z < total_zero_count; z++ {
			product.value = append(product.value, uint8(0))
		}
		return product

	} else if trailing_zero_cnt_big_int > 0 {
		// if only big_int has trailing zeros, gets muliplication without them then adds them back to the product and returns said product

		var value_str string = ""
		for index := 0; index < len(value_arr_big_int); index++ {
			value_str += strconv.FormatInt(int64(value_arr_big_int[index]), 10)
		}
		big_int_no_zeros := big_int.GetCopy()
		big_int_no_zeros.SetValue(value_str[:len(value_str)-trailing_zero_cnt_big_int])

		product = big_int_no_zeros.Multiply(other_big_int)
		var total_zero_count int = trailing_zero_cnt_big_int + trailing_zero_cnt_other_big_int
		for z := 0; z < total_zero_count; z++ {
			product.value = append(product.value, uint8(0))
		}
		return product

	} else if trailing_zero_cnt_other_big_int > 0 {
		// if only other_big_int has trailing zeros, gets muliplication without them then adds them back to the product and returns said product

		var value_str string = ""
		for index := 0; index < len(value_arr_other_big_int); index++ {
			value_str += strconv.FormatInt(int64(value_arr_other_big_int[index]), 10)
		}
		other_big_int_no_zeros := other_big_int.GetCopy()
		other_big_int_no_zeros.SetValue(value_str[:len(value_str)-trailing_zero_cnt_other_big_int])

		product = big_int.Multiply(other_big_int_no_zeros)
		var total_zero_count int = trailing_zero_cnt_big_int + trailing_zero_cnt_other_big_int
		for z := 0; z < total_zero_count; z++ {
			product.value = append(product.value, uint8(0))
		}
		return product
	}

	// adds big int to product other big int amount of times
	// with the optimization rate, this basically multiplies
	// everythng by the optimization rate when doing addition,
	// therfore dividing the amount of additions by the optimization rate
	// by default the optimization rate is set to the maximum value
	// of an int, however the optimization rate can be any value
	// that is greater than 0 but less than the max value for an uint
	// note: actual math is done using absolute values
	counter := NewBigInt()
	var optimization_rate uint = math.MaxInt
	for counter.IsLessThan(other_big_int.Abs().Subtract(optimization_rate)) {
		counter = counter.Add(optimization_rate)
		product = product.Add(big_int.Abs().Multiply(optimization_rate))
	}

	// adds big int to product other big int amount of times
	// without optimization factor, this makes sure the additions
	// that are less than the optimization rate still process
	// note: actual math is done using absolute values
	for counter.IsLessThan(other_big_int.Abs()) {
		counter = counter.Add(1)
		product = product.Add(big_int.Abs())
	}

	// sets the sign of the new big int
	// same sign is positive, different signs is negative
	if !(big_int.IsPositive() == other_big_int.IsPositive()) {
		product = product.GetSwitchedSign()
	}

	// return the product
	return product
}

// redirects to DivideBigInt with the correct input
func (big_int *BigInt) Divide(i interface{}) *BigInt {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt pointer, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.DivideBigInt(i.(*BigInt))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.DivideBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.DivideBigInt(NewBigInt())
}

// returns a BigInt that represents the quotient of two BigInts
func (big_int *BigInt) DivideBigInt(other_big_int *BigInt) *BigInt {

	// divides using built-in math if capable
	// checks if each is less than or equal to
	// the max value for an uint or an int if
	// at least one BigInt is negative
	if big_int.IsPositive() && other_big_int.IsPositive() {
		var max_value uint = math.MaxUint
		if big_int.IsLessThanOrEqualTo(max_value) && other_big_int.IsLessThanOrEqualTo(max_value) {
			return NewBigInt(big_int.GetUnsignedInt() / other_big_int.GetUnsignedInt())
		}
	} else {
		var max_value int = math.MaxInt
		if big_int.IsLessThanOrEqualTo(max_value) && other_big_int.IsLessThanOrEqualTo(max_value) {
			return NewBigInt(big_int.GetInt() / other_big_int.GetInt())
		}
	}

	// if big int is less than other big int, return 0
	if big_int.IsLessThan(other_big_int) {
		return NewBigInt()
	}

	// counts number of trailing zeros in big int
	var value_arr_big_int []uint8 = big_int.value
	var trailing_zero_cnt_big_int int = 0
	for index := len(value_arr_big_int) - 1; index >= 0; index-- {
		if value_arr_big_int[index] == uint8(0) {
			trailing_zero_cnt_big_int++
		} else {
			break
		}
	}

	// counts number of trailing zeros in other big int
	var value_arr_other_big_int []uint8 = other_big_int.value
	var trailing_zero_cnt_other_big_int int = 0
	for index := len(value_arr_other_big_int) - 1; index >= 0; index-- {
		if value_arr_other_big_int[index] == uint8(0) {
			trailing_zero_cnt_other_big_int++
		} else {
			break
		}
	}

	// creates the big int to return
	quotient := NewBigInt()

	// if there both BigInts have trailing zeros, gets division without them then adds them back to the quotient and returns said quotient
	if trailing_zero_cnt_big_int > 0 && trailing_zero_cnt_other_big_int > 0 {

		var value_str string = ""
		for index := 0; index < len(value_arr_big_int); index++ {
			value_str += strconv.FormatInt(int64(value_arr_big_int[index]), 10)
		}
		big_int_no_zeros := big_int.GetCopy()
		big_int_no_zeros.SetValue(value_str[:len(value_str)-trailing_zero_cnt_big_int])

		value_str = ""
		for index := 0; index < len(value_arr_other_big_int); index++ {
			value_str += strconv.FormatInt(int64(value_arr_other_big_int[index]), 10)
		}
		other_big_int_no_zeros := other_big_int.GetCopy()
		other_big_int_no_zeros.SetValue(value_str[:len(value_str)-trailing_zero_cnt_other_big_int])

		quotient = big_int_no_zeros.Divide(other_big_int_no_zeros)
		var total_zero_count int = trailing_zero_cnt_big_int + trailing_zero_cnt_other_big_int
		for z := 0; z < total_zero_count; z++ {
			quotient.value = append(quotient.value, uint8(0))
		}
		return quotient

	} else if trailing_zero_cnt_big_int > 0 {
		// if only big_int has trailing zeros, gets division without them then adds them back to the quotient and returns said quotient

		var value_str string = ""
		for index := 0; index < len(value_arr_big_int); index++ {
			value_str += strconv.FormatInt(int64(value_arr_big_int[index]), 10)
		}
		big_int_no_zeros := big_int.GetCopy()
		big_int_no_zeros.SetValue(value_str[:len(value_str)-trailing_zero_cnt_big_int])

		quotient = big_int_no_zeros.Divide(other_big_int)
		var total_zero_count int = trailing_zero_cnt_big_int + trailing_zero_cnt_other_big_int
		for z := 0; z < total_zero_count; z++ {
			quotient.value = append(quotient.value, uint8(0))
		}
		return quotient

	} else if trailing_zero_cnt_other_big_int > 0 {
		// if only other_big_int has trailing zeros, gets division without them then adds them back to the quotient and returns said quotient

		var value_str string = ""
		for index := 0; index < len(value_arr_other_big_int); index++ {
			value_str += strconv.FormatInt(int64(value_arr_other_big_int[index]), 10)
		}
		other_big_int_no_zeros := other_big_int.GetCopy()
		other_big_int_no_zeros.SetValue(value_str[:len(value_str)-trailing_zero_cnt_other_big_int])

		quotient = big_int.Divide(other_big_int_no_zeros)
		var total_zero_count int = trailing_zero_cnt_big_int + trailing_zero_cnt_other_big_int
		for z := 0; z < total_zero_count; z++ {
			quotient.value = append(quotient.value, uint8(0))
		}
		return quotient
	}

	// creates the big int to track the subtractions
	diff_bigint := big_int.GetCopy()

	// subtracts big int to product other big int amount of times
	// with the optimization rate, this basically multiplies
	// everythng by the optimization rate when doing subtraction,
	// therfore dividing the amount of subtractions by the optimization rate
	// by default the optimization rate is set to the maximum value
	// of an int, however the optimization rate can be any value
	// that is greater than 0 but less than the max value for an uint
	// note: actual math is done using absolute values
	counter := NewBigInt()
	var optimization_rate uint = math.MaxInt
	for counter.IsLessThan(other_big_int.Abs().Multiply(optimization_rate)) {
		counter = counter.Add(optimization_rate)
		diff_bigint = diff_bigint.Subtract(big_int.Abs().Multiply(optimization_rate))
	}

	// adds big int to product other big int amount of times
	// without optimization factor, this makes sure the additions
	// that are less than the optimization rate still process
	// note: actual math is done using absolute values
	for counter.IsLessThan(other_big_int.Abs()) {
		counter = counter.Add(1)
		diff_bigint = diff_bigint.Subtract(big_int.Abs())
	}

	// sets the sign of the new big int
	// same sign is positive, different signs is negative
	if !(big_int.IsPositive() == other_big_int.IsPositive()) {
		counter = counter.GetSwitchedSign()
	}

	// return the quotient
	// note: counter and quotient will always be same value
	// so just using counter instead of creating a new
	// BigInt
	return counter
}

// redirects to ModuloBigInt with the correct input
func (big_int *BigInt) Modulo(i interface{}) *BigInt {
	// switches through the possible input types
	switch reflect.TypeOf(i) {

	// if input is a BigInt pointer, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return big_int.ModuloBigInt(i.(*BigInt))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return big_int.ModuloBigInt(NewBigInt(i.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return big_int.ModuloBigInt(NewBigInt())
}

// returns a BigInt that represents the remainder of one BigInt divided by another
func (big_int *BigInt) ModuloBigInt(other_big_int *BigInt) *BigInt {

	// if big int less than other big int, return 0
	if big_int.IsLessThan(other_big_int) {
		return NewBigInt()
	}

	// modulos using built-in math if capable
	// checks if each is less than or equal to
	// the max value for an uint or an int if
	// at least one BigInt is negative
	if big_int.IsPositive() && other_big_int.IsPositive() {
		var max_value uint = math.MaxUint
		if big_int.IsLessThanOrEqualTo(max_value) {
			if other_big_int.IsLessThanOrEqualTo(max_value) {
				return NewBigInt(big_int.GetUnsignedInt() % other_big_int.GetUnsignedInt())
			}
		}
	} else {
		var max_value int = math.MaxInt
		if big_int.IsLessThanOrEqualTo(max_value) {
			if other_big_int.IsLessThanOrEqualTo(max_value) {
				return NewBigInt(big_int.GetInt() % other_big_int.GetInt())
			}
		}
	}

	// modulo = bigger number - (smaller number * (bigger number / smaller number) )
	// big_int is assumed to be the bigger number, otherwise 0 would have already been returned
	return big_int.Subtract(other_big_int.Multiply(big_int.Divide(other_big_int))).GetCopy()
}

// redirects to PowBigInt with the correct input
func (base *BigInt) Pow(power interface{}) *BigInt {
	// switches through the possible input types
	switch reflect.TypeOf(power) {

	// if input is a BigInt pointer, redirect it as is
	case reflect.TypeOf(NewBigInt()):
		{
			return base.PowBigInt(power.(*BigInt))
		}
	// if input is an int, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int(0)):
		{
			return base.PowBigInt(NewBigInt(power.(int)))
		}
	// if input is an int8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int8(0)):
		{
			return base.PowBigInt(NewBigInt(power.(int8)))
		}
	// if input is an int16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int16(0)):
		{
			return base.PowBigInt(NewBigInt(power.(int16)))
		}
	// if input is an int32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int32(0)):
		{
			return base.PowBigInt(NewBigInt(power.(int32)))
		}
	// if input is an int64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(int64(0)):
		{
			return base.PowBigInt(NewBigInt(power.(int64)))
		}
	// if input is an uint, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint(0)):
		{
			return base.PowBigInt(NewBigInt(power.(uint)))
		}
	// if input is an uint8, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint8(0)):
		{
			return base.PowBigInt(NewBigInt(power.(uint8)))
		}
	// if input is an uint16, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint16(0)):
		{
			return base.PowBigInt(NewBigInt(power.(uint16)))
		}
	// if input is an uint32, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint32(0)):
		{
			return base.PowBigInt(NewBigInt(power.(uint32)))
		}
	// if input is an uint64, convert it to a BigInt, then redirect it
	case reflect.TypeOf(uint64(0)):
		{
			return base.PowBigInt(NewBigInt(power.(uint64)))
		}

	}
	// if the instance type does not fit into any of the above cases,
	// then redirect with a new BigInt as input
	return base.PowBigInt(NewBigInt())
}

// returns a BigInt that represents one BigInt to the power of another
func (base *BigInt) PowBigInt(power *BigInt) *BigInt {

	// if the exponent is negative, performs the proper redirect
	if !power.IsPositive() {
		return NewBigInt(1).Divide(base.Pow(power.Abs()))
	}

	// if big int less than other big int, return 0
	if base.IsLessThan(power) {
		return NewBigInt()
	}

	// powers using built-in math if capable
	// checks if each is less than or equal to 15
	// as 16^16 is equal to 2^64 which is greater than
	// the uint and int ranges. 15^15 however, falls between
	// both ranges
	if base.IsPositive() && power.IsPositive() {
		var max_value uint = 15
		if base.IsLessThanOrEqualTo(max_value) {
			if power.IsLessThanOrEqualTo(max_value) {
				return NewBigInt(uint(math.Pow(float64(base.GetUnsignedInt()), float64(power.GetUnsignedInt()))))
			}
		}
	} else {
		var max_value int = 15
		if base.IsLessThanOrEqualTo(max_value) {
			if power.IsLessThanOrEqualTo(max_value) {
				return NewBigInt(int(math.Pow(float64(base.GetInt()), float64(power.GetInt()))))
			}
		}
	}

	// creates big int to hold the product
	product := NewBigInt(1)

	// multiplies product by big int other big int times
	counter := NewBigInt()
	for counter.IsLessThan(power) {
		counter = counter.Add(1)
		product = product.Multiply(base)
	}

	// returns product
	return product
}

// returns a BigInt that represents a BigInt factorial
func (big_int *BigInt) Factorial() *BigInt {

	// creates big int to return
	product := NewBigInt(1)

	// calculates the factorial
	// for example, 5 factorial is equal to
	// 5 * 4 * 3 * 2 * 1
	factor := big_int.GetCopy()
	for factor.IsGreaterThanOrEqualTo(1) {
		product = product.Multiply(factor)
		factor = factor.Subtract(1)
	}

	// returns the product, aka the factorial of a BigInt
	return product
}
