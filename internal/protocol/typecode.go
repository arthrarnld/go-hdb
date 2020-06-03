/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protocol

import (
	"fmt"
	"strings"
)

//go:generate stringer -type=typeCode

// typeCode identify the type of a field transferred to or from the database.
type typeCode byte

// null value indicator is high bit

//nolint
const (
	tcNullL             typeCode = 0x00
	tcTinyint           typeCode = 0x01
	tcSmallint          typeCode = 0x02
	tcInteger           typeCode = 0x03
	tcBigint            typeCode = 0x04
	tcDecimal           typeCode = 0x05
	tcReal              typeCode = 0x06
	tcDouble            typeCode = 0x07
	tcChar              typeCode = 0x08
	tcVarchar           typeCode = 0x09 // changed from tcVarchar1 to tcVarchar (ref hdbclient)
	tcNchar             typeCode = 0x0A
	tcNvarchar          typeCode = 0x0B
	tcBinary            typeCode = 0x0C
	tcVarbinary         typeCode = 0x0D
	tcDate              typeCode = 0x0E
	tcTime              typeCode = 0x0F
	tcTimestamp         typeCode = 0x10
	tcTimetz            typeCode = 0x11
	tcTimeltz           typeCode = 0x12
	tcTimestampTz       typeCode = 0x13
	tcTimestampLtz      typeCode = 0x14
	tcIntervalYm        typeCode = 0x15
	tcIntervalDs        typeCode = 0x16
	tcRowid             typeCode = 0x17
	tcUrowid            typeCode = 0x18
	tcClob              typeCode = 0x19
	tcNclob             typeCode = 0x1A
	tcBlob              typeCode = 0x1B
	tcBoolean           typeCode = 0x1C
	tcString            typeCode = 0x1D
	tcNstring           typeCode = 0x1E
	tcLocator           typeCode = 0x1F
	tcNlocator          typeCode = 0x20
	tcBstring           typeCode = 0x21
	tcDecimalDigitArray typeCode = 0x22
	tcVarchar2          typeCode = 0x23
	tcTable             typeCode = 0x2D
	tcSmalldecimal      typeCode = 0x2f // inserted (not existent in hdbclient)
	tcAbapstream        typeCode = 0x30
	tcAbapstruct        typeCode = 0x31
	tcAarray            typeCode = 0x32
	tcText              typeCode = 0x33
	tcShorttext         typeCode = 0x34
	tcBintext           typeCode = 0x35
	tcAlphanum          typeCode = 0x37
	tcLongdate          typeCode = 0x3D
	tcSeconddate        typeCode = 0x3E
	tcDaydate           typeCode = 0x3F
	tcSecondtime        typeCode = 0x40
	tcClocator          typeCode = 0x46
	tcBlobDiskReserved  typeCode = 0x47
	tcClobDiskReserved  typeCode = 0x48
	tcNclobDiskReserved typeCode = 0x49
	tcStGeometry        typeCode = 0x4A
	tcStPoint           typeCode = 0x4B
	tcFixed16           typeCode = 0x4C
	tcAbapItab          typeCode = 0x4D
	tcRecordRowStore    typeCode = 0x4E
	tcRecordColumnStore typeCode = 0x4F
	tcFixed8            typeCode = 0x51
	tcFixed12           typeCode = 0x52
	tcCiphertext        typeCode = 0x5A

	// additional internal typecodes
	tcTableRef  typeCode = 0x7e // 126
	tcTableRows typeCode = 0x7f // 127
)

func (tc typeCode) isLob() bool {
	return tc == tcClob || tc == tcNclob || tc == tcBlob || tc == tcText || tc == tcBintext || tc == tcLocator
}

func (tc typeCode) isCharBased() bool {
	return tc == tcNvarchar || tc == tcNstring || tc == tcNclob || tc == tcText || tc == tcBintext
}

func (tc typeCode) isVariableLength() bool {
	return tc == tcChar || tc == tcNchar || tc == tcVarchar || tc == tcNvarchar || tc == tcBinary || tc == tcVarbinary || tc == tcShorttext || tc == tcAlphanum
}

func (tc typeCode) isIntegerType() bool {
	return tc == tcTinyint || tc == tcSmallint || tc == tcInteger || tc == tcBigint
}

func (tc typeCode) isDecimalType() bool {
	return tc == tcSmalldecimal || tc == tcDecimal
}

// see hdbclient
func (tc typeCode) encTc() typeCode {
	switch tc {
	default:
		return tc
	case tcText, tcBintext, tcLocator:
		return tcNclob
	}
}

/*
tcBintext:
- protocol returns tcLocator for tcBintext
- see dataTypeMap and encTc
*/

var dataTypeMap = map[typeCode]DataType{
	tcTinyint:    DtTinyint,
	tcSmallint:   DtSmallint,
	tcInteger:    DtInteger,
	tcBigint:     DtBigint,
	tcReal:       DtReal,
	tcDouble:     DtDouble,
	tcDate:       DtTime,
	tcTime:       DtTime,
	tcTimestamp:  DtTime,
	tcLongdate:   DtTime,
	tcSeconddate: DtTime,
	tcDaydate:    DtTime,
	tcSecondtime: DtTime,
	tcDecimal:    DtDecimal,
	tcChar:       DtString,
	tcVarchar:    DtString,
	tcString:     DtString,
	tcAlphanum:   DtString,
	tcNchar:      DtString,
	tcNvarchar:   DtString,
	tcNstring:    DtString,
	tcShorttext:  DtString,
	tcBinary:     DtBytes,
	tcVarbinary:  DtBytes,
	tcBlob:       DtLob,
	tcClob:       DtLob,
	tcNclob:      DtLob,
	tcText:       DtLob,
	tcBintext:    DtLob,
	tcTableRef:   DtString,
	tcTableRows:  DtRows,
}

// DataType converts a type code into one of the supported data types by the driver.
func (tc typeCode) dataType() DataType {
	dt, ok := dataTypeMap[tc]
	if !ok {
		panic(fmt.Sprintf("Missing DataType for typeCode %s", tc))
	}
	return dt
}

// typeName returns the database type name.
// see https://golang.org/pkg/database/sql/driver/#RowsColumnTypeDatabaseTypeName
func (tc typeCode) typeName() string {
	return strings.ToUpper(tc.String()[2:])
}

var tcFieldTypeMap = map[typeCode]fieldType{
	tcTinyint:    tinyintType,
	tcSmallint:   smallintType,
	tcInteger:    integerType,
	tcBigint:     bigintType,
	tcReal:       realType,
	tcDouble:     doubleType,
	tcDate:       dateType,
	tcTime:       timeType,
	tcTimestamp:  timestampType,
	tcLongdate:   longdateType,
	tcSeconddate: seconddateType,
	tcDaydate:    daydateType,
	tcSecondtime: secondtimeType,
	tcDecimal:    decimalType,
	tcChar:       varType,
	tcVarchar:    varType,
	tcString:     varType,
	tcAlphanum:   alphaType,
	tcNchar:      cesu8Type,
	tcNvarchar:   cesu8Type,
	tcNstring:    cesu8Type,
	tcShorttext:  cesu8Type,
	tcBinary:     varType,
	tcVarbinary:  varType,
	tcBlob:       lobVarType,
	tcClob:       lobVarType,
	tcNclob:      lobCESU8Type,
	tcText:       lobCESU8Type,
	tcBintext:    lobCESU8Type,
	tcLocator:    lobCESU8Type,
}

func (tc typeCode) fieldType() fieldType {
	f, ok := tcFieldTypeMap[tc]
	if !ok {
		panic(fmt.Sprintf("Missing FieldType for typeCode %s", tc))
	}
	return f
}
