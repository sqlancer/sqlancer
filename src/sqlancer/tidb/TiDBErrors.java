package sqlancer.tidb;

import sqlancer.common.query.ExpectedErrors;

public final class TiDBErrors {

    private TiDBErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("DECIMAL value is out of range");
        errors.add("error parsing regexp");
        errors.add("BIGINT UNSIGNED value is out of range");
        errors.add("Data truncation: Truncated incorrect time value");
        errors.add("Data truncation: Incorrect time value");
        errors.add("overflows double");
        errors.add("overflows bigint");
        errors.add("strconv.ParseFloat: parsing");
        errors.add("in 'order clause'"); // int constants in order by clause

        // functions
        errors.add("BIGINT value is out of range");
        errors.add("doesn't have a default value"); // default

        errors.add("is not valid for CHARACTER SET");

        // known issue: https://github.com/pingcap/tidb/issues/14819
        // errors.add("Wrong plan type for dataReaderBuilder");

        errors.add("DOUBLE value is out of range");

        // errors.add("index out of range"); // https://github.com/pingcap/tidb/issues/15810
        // errors.add("baseBuiltinFunc.evalString() should never be called, please contact the TiDB team for help"); //
        // https://github.com/pingcap/tidb/issues/15847
        // errors.add("unsupport column type for encode 6"); // https://github.com/pingcap/tidb/issues/15850

        errors.add("Data truncation: %s value is out of range in '%s'");
        errors.add("Truncated incorrect FLOAT value");
        errors.add("Bad Number");

        // regex
        errors.add("error parsing regexp");
        errors.add("from regexp");

        // To avoid bugs
        errors.add("Unknown column"); // https://github.com/pingcap/tidb/issues/35522
        errors.add("Can\'t find column"); // https://github.com/pingcap/tidb/issues/35527
        errors.add("Cannot convert"); // https://github.com/pingcap/tidb/issues/35652

        // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/57
        // errors.add("For input string: \"+Inf\"");

        // errors.add("inconsistent index"); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/58

        // errors.add("Illegal mix of collations");

        if (TiDBBugs.bug35677) {
            errors.add("for function inet_aton");
        }
        if (TiDBBugs.bug35522) {
            errors.add("ERROR 1054 (42S22)");
        }
        if (TiDBBugs.bug35652) {
            errors.add("from binary to utf8");
        }
        if (TiDBBugs.bug38295) {
            errors.add("assertion failed");
        }
    }

    public static void addExpressionHavingErrors(ExpectedErrors errors) {
        errors.add("is not in GROUP BY clause and contains nonaggregated column");
        errors.add("Unknown column");
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add("Duplicate entry");
        errors.add("cannot be null");
        errors.add("doesn't have a default value");
        errors.add("Out of range value");
        errors.add("Incorrect double value");
        errors.add("Incorrect float value");
        errors.add("Incorrect int value");
        errors.add("Incorrect tinyint value");
        errors.add("Data truncation");
        errors.add("Bad Number");
        errors.add("The value specified for generated column"); // TODO: do not insert data into generated columns
        errors.add("incorrect utf8 value");
        errors.add("Data truncation: %s value is out of range in '%s'");
        errors.add("Incorrect smallint value");
        errors.add("Incorrect bigint value");
        errors.add("Incorrect decimal value");
        errors.add("error parsing regexp");
        errors.add("is not valid for CHARACTER SET");

        // if (true) {
        // // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/54
        // errors.add("Miss column");
        // }
    }

}
