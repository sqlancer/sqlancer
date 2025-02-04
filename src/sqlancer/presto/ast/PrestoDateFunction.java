package sqlancer.presto.ast;

import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;

public enum PrestoDateFunction implements PrestoFunction {

    // Date and Time Functions#
    // Returns the current date as of the start of the query.
    CURRENT_DATE("current_date", PrestoDataType.DATE),

    // Returns the current time as of the start of the query.
    CURRENT_TIME("current_time", PrestoDataType.TIME_WITH_TIME_ZONE),

    // Returns the current timestamp as of the start of the query.
    CURRENT_TIMESTAMP("current_timestamp", PrestoDataType.TIMESTAMP_WITH_TIME_ZONE),

    // Returns the current time zone in the format defined by IANA (e.g., America/Los_Angeles) or as fixed offset from
    // UTC (e.g., +08:35)
    CURRENT_TIMEZONE("current_timezone", PrestoDataType.VARCHAR),

    // This is an alias for CAST(x AS date).
    DATE("date", PrestoDataType.DATE, PrestoDataType.DATE, PrestoDataType.INT, PrestoDataType.VARCHAR),

    // Returns the last day of the month.
    LAST_DAY_OF_MONTH("last_day_of_month", PrestoDataType.DATE, PrestoDataType.DATE),

    // Parses the ISO 8601 formatted string into a timestamp with time zone.
    FROM_ISO8601_TIMESTAMP("from_iso8601_timestamp", PrestoDataType.TIMESTAMP_WITH_TIME_ZONE, PrestoDataType.VARCHAR),

    // Parses the ISO 8601 formatted string into a date.
    FROM_ISO8601_DATE("from_iso8601_date", PrestoDataType.DATE, PrestoDataType.VARCHAR),

    // Returns the UNIX timestamp unixtime as a timestamp.
    FROM_UNIXTIME("from_unixtime", PrestoDataType.TIMESTAMP, PrestoDataType.INT),

    // Returns the UNIX timestamp unixtime as a timestamp with time zone using string for the time zone.
    FROM_UNIXTIME_TIMEZONE("from_unixtime", PrestoDataType.TIMESTAMP_WITH_TIME_ZONE, PrestoDataType.INT,
            PrestoDataType.VARCHAR) {
        @Override
        public boolean shouldPreserveOrderOfArguments() {
            return true;
        }
    },

    // Returns the UNIX timestamp unixtime as a timestamp with time zone using hours and minutes for the time zone
    // offset.
    FROM_UNIXTIME_HOURS_MINUTES("from_unixtime", PrestoDataType.TIMESTAMP_WITH_TIME_ZONE, PrestoDataType.INT,
            PrestoDataType.INT) {
        @Override
        public boolean shouldPreserveOrderOfArguments() {
            return true;
        }
    },

    // Returns the current time as of the start of the query. -> time
    LOCALTIME("localtime", PrestoDataType.TIME),

    // Returns the current timestamp as of the start of the query. -> timestamp
    LOCALTIMESTAMP("localtimestamp", PrestoDataType.TIMESTAMP),

    // This is an alias for current_timestamp. → timestamp with time zone#
    NOW("now", PrestoDataType.TIMESTAMP_WITH_TIME_ZONE),

    // Formats x as an ISO 8601 string. x can be date, timestamp, or timestamp with time zone. → varchar#
    TO_ISO8601("to_iso8601", PrestoDataType.VARCHAR, PrestoDataType.DATE, PrestoDataType.TIMESTAMP,
            PrestoDataType.TIMESTAMP_WITH_TIME_ZONE),

    // Returns the day-to-second interval as milliseconds. → bigint#
    TO_MILLISECONDS("to_milliseconds", PrestoDataType.INT, PrestoDataType.INTERVAL_DAY_TO_SECOND),
    TO_MILLISECONDS_2("to_milliseconds", PrestoDataType.INT, PrestoDataType.INTERVAL_YEAR_TO_MONTH),

    // Returns timestamp as a UNIX timestamp. → double#
    TO_UNIXTIME("to_unixtime", PrestoDataType.FLOAT, PrestoDataType.TIMESTAMP),
    TO_UNIXTIME_2("to_unixtime", PrestoDataType.FLOAT, PrestoDataType.TIMESTAMP_WITH_TIME_ZONE),

    // The following SQL-standard functions do not use parenthesis:
    CURRENT_DATE_NA("current_date", PrestoDataType.DATE) {
        @Override
        public boolean isStandardFunction() {
            return false;
        }
    },

    CURRENT_TIME_NA("current_time", PrestoDataType.TIME) {
        @Override
        public boolean isStandardFunction() {
            return false;
        }
    },

    CURRENT_TIMESTAMP_NA("current_timestamp", PrestoDataType.TIMESTAMP) {
        @Override
        public boolean isStandardFunction() {
            return false;
        }
    },

    LOCALTIME_NA("localtime", PrestoDataType.TIME) {
        @Override
        public boolean isStandardFunction() {
            return false;
        }
    },

    LOCALTIMESTAMP_NA("localtimestamp", PrestoDataType.TIMESTAMP) {
        @Override
        public boolean isStandardFunction() {
            return false;
        }
    },

    // Truncation Function
    // date_trunc(unit, x) → [same as input]
    DATE_TRUNC_1("date_trunc", PrestoDataType.TIMESTAMP, PrestoDataType.VARCHAR, PrestoDataType.TIMESTAMP),
    DATE_TRUNC_2("date_trunc", PrestoDataType.TIMESTAMP_WITH_TIME_ZONE, PrestoDataType.VARCHAR,
            PrestoDataType.TIMESTAMP_WITH_TIME_ZONE),
    DATE_TRUNC_3("date_trunc", PrestoDataType.DATE, PrestoDataType.VARCHAR, PrestoDataType.DATE),
    DATE_TRUNC_4("date_trunc", PrestoDataType.TIME, PrestoDataType.VARCHAR, PrestoDataType.TIME);

    /*
     *
     * Interval Functions# The functions in this section support the following interval units:
     *
     * Unit
     *
     * Description
     *
     * millisecond
     *
     * Milliseconds
     *
     * second
     *
     * Seconds
     *
     * minute
     *
     * Minutes
     *
     * hour
     *
     * Hours
     *
     * day
     *
     * Days
     *
     * week
     *
     * Weeks
     *
     * month
     *
     * Months
     *
     * quarter
     *
     * Quarters of a year
     *
     * year
     *
     * Years
     *
     * date_add(unit, value, timestamp) → [same as input]# Adds an interval value of type unit to timestamp. Subtraction
     * can be performed by using a negative value.
     *
     * date_diff(unit, timestamp1, timestamp2) → bigint# Returns timestamp2 - timestamp1 expressed in terms of unit.
     *
     * Duration Function# The parse_duration function supports the following units:
     *
     * Unit
     *
     * Description
     *
     * ns
     *
     * Nanoseconds
     *
     * us
     *
     * Microseconds
     *
     * ms
     *
     * Milliseconds
     *
     * s
     *
     * Seconds
     *
     * m
     *
     * Minutes
     *
     * h
     *
     * Hours
     *
     * d
     *
     * Days
     *
     * parse_duration(string) → interval# Parses string of format value unit into an interval, where value is fractional
     * number of unit values:
     *
     * SELECT parse_duration('42.8ms'); -- 0 00:00:00.043 SELECT parse_duration('3.81 d'); -- 3 19:26:24.000 SELECT
     * parse_duration('5m'); -- 0 00:05:00.000 MySQL Date Functions# The functions in this section use a format string
     * that is compatible with the MySQL date_parse and str_to_date functions. The following table, based on the MySQL
     * manual, describes the format specifiers:
     *
     * Specifier
     *
     * Description
     *
     * %a
     *
     * Abbreviated weekday name (Sun .. Sat)
     *
     * %b
     *
     * Abbreviated month name (Jan .. Dec)
     *
     * %c
     *
     * Month, numeric (1 .. 12) 4
     *
     * %D
     *
     * Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
     *
     * %d
     *
     * Day of the month, numeric (01 .. 31) 4
     *
     * %e
     *
     * Day of the month, numeric (1 .. 31) 4
     *
     * %f
     *
     * Fraction of second (6 digits for printing: 000000 .. 999000; 1 - 9 digits for parsing: 0 .. 999999999) 1
     *
     * %H
     *
     * Hour (00 .. 23)
     *
     * %h
     *
     * Hour (01 .. 12)
     *
     * %I
     *
     * Hour (01 .. 12)
     *
     * %i
     *
     * Minutes, numeric (00 .. 59)
     *
     * %j
     *
     * Day of year (001 .. 366)
     *
     * %k
     *
     * Hour (0 .. 23)
     *
     * %l
     *
     * Hour (1 .. 12)
     *
     * %M
     *
     * Month name (January .. December)
     *
     * %m
     *
     * Month, numeric (01 .. 12) 4
     *
     * %p
     *
     * AM or PM
     *
     * %r
     *
     * Time, 12-hour (hh:mm:ss followed by AM or PM)
     *
     * %S
     *
     * Seconds (00 .. 59)
     *
     * %s
     *
     * Seconds (00 .. 59)
     *
     * %T
     *
     * Time, 24-hour (hh:mm:ss)
     *
     * %U
     *
     * Week (00 .. 53), where Sunday is the first day of the week
     *
     * %u
     *
     * Week (00 .. 53), where Monday is the first day of the week
     *
     * %V
     *
     * Week (01 .. 53), where Sunday is the first day of the week; used with %X
     *
     * %v
     *
     * Week (01 .. 53), where Monday is the first day of the week; used with %x
     *
     * %W
     *
     * Weekday name (Sunday .. Saturday)
     *
     * %w
     *
     * Day of the week (0 .. 6), where Sunday is the first day of the week 3
     *
     * %X
     *
     * Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
     *
     * %x
     *
     * Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
     *
     * %Y
     *
     * Year, numeric, four digits
     *
     * %y
     *
     * Year, numeric (two digits) 2
     *
     * %%
     *
     * A literal % character
     *
     * %x
     *
     * x, for any x not listed above
     *
     * 1 Timestamp is truncated to milliseconds.
     *
     * 2 When parsing, two-digit year format assumes range 1970 ... 2069, so “70” will result in year 1970 but “69” will
     * produce 2069.
     *
     * 3 This specifier is not supported yet. Consider using day_of_week() (it uses 1-7 instead of 0-6).
     *
     * 4(1,2,3,4) This specifier does not support 0 as a month or day.
     *
     * Warning
     *
     * The following specifiers are not currently supported: %D %U %u %V %w %X
     *
     * date_format(timestamp, format) → varchar# Formats timestamp as a string using format.
     *
     * date_parse(string, format) → timestamp# Parses string into a timestamp using format.
     *
     * Java Date Functions# The functions in this section use a format string that is compatible with JodaTime’s
     * DateTimeFormat pattern format.
     *
     * format_datetime(timestamp, format) → varchar# Formats timestamp as a string using format.
     *
     * parse_datetime(string, format) → timestamp with time zone# Parses string into a timestamp with time zone using
     * format.
     *
     * Extraction Function# The extract function supports the following fields:
     *
     * Field
     *
     * Description
     *
     * YEAR
     *
     * year()
     *
     * QUARTER
     *
     * quarter()
     *
     * MONTH
     *
     * month()
     *
     * WEEK
     *
     * week()
     *
     * DAY
     *
     * day()
     *
     * DAY_OF_MONTH
     *
     * day()
     *
     * DAY_OF_WEEK
     *
     * day_of_week()
     *
     * DOW
     *
     * day_of_week()
     *
     * DAY_OF_YEAR
     *
     * day_of_year()
     *
     * DOY
     *
     * day_of_year()
     *
     * YEAR_OF_WEEK
     *
     * year_of_week()
     *
     * YOW
     *
     * year_of_week()
     *
     * HOUR
     *
     * hour()
     *
     * MINUTE
     *
     * minute()
     *
     * SECOND
     *
     * second()
     *
     * TIMEZONE_HOUR
     *
     * timezone_hour()
     *
     * TIMEZONE_MINUTE
     *
     * timezone_minute()
     *
     * The types supported by the extract function vary depending on the field to be extracted. Most fields support all
     * date and time types.
     *
     * extract(field FROM x) → bigint# Returns field from x.
     *
     * Note
     *
     * This SQL-standard function uses special syntax for specifying the arguments.
     *
     * Convenience Extraction Functions# day(x) → bigint# Returns the day of the month from x.
     *
     * day_of_month(x) → bigint# This is an alias for day().
     *
     * day_of_week(x) → bigint# Returns the ISO day of the week from x. The value ranges from 1 (Monday) to 7 (Sunday).
     *
     * day_of_year(x) → bigint# Returns the day of the year from x. The value ranges from 1 to 366.
     *
     * dow(x) → bigint# This is an alias for day_of_week().
     *
     * doy(x) → bigint# This is an alias for day_of_year().
     *
     * hour(x) → bigint# Returns the hour of the day from x. The value ranges from 0 to 23.
     *
     * millisecond(x) → bigint# Returns the millisecond of the second from x.
     *
     * minute(x) → bigint# Returns the minute of the hour from x.
     *
     * month(x) → bigint# Returns the month of the year from x.
     *
     * quarter(x) → bigint# Returns the quarter of the year from x. The value ranges from 1 to 4.
     *
     * second(x) → bigint# Returns the second of the minute from x.
     *
     * timezone_hour(timestamp) → bigint# Returns the hour of the time zone offset from timestamp.
     *
     * timezone_minute(timestamp) → bigint# Returns the minute of the time zone offset from timestamp.
     *
     * week(x) → bigint# Returns the ISO week of the year from x. The value ranges from 1 to 53.
     *
     * week_of_year(x) → bigint# This is an alias for week().
     *
     * year(x) → bigint# Returns the year from x.
     *
     * year_of_week(x) → bigint# Returns the year of the ISO week from x.
     *
     * yow(x) → bigint# This is an alias for year_of_week().
     *
     *
     *
     */

    private final PrestoDataType returnType;
    private final PrestoDataType[] argumentTypes;
    private final String functionName;

    PrestoDateFunction(String functionName, PrestoDataType returnType, PrestoDataType... argumentTypes) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes.clone();
    }

    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
        return this.returnType == returnType.getPrimitiveDataType();
    }

    @Override
    public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
        return argumentTypes.clone();
    }

}
