package lama.schema;

public enum PrimitiveDataType {
	NULL,
	INT,
	TEXT,
	REAL,
 NONE,
	DATETIME, SET //mysql
	, ENUM //mysql
, BINARY // mysql
, UNINTERPRETED // do not escape current_time etc. in the visitor
}
 