package sqlancer.sqlite3.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public interface SQLite3Expression extends Expression<SQLite3Column> {

    default SQLite3Constant getExpectedValue() {
        return null;
    }

    default SQLite3TypeAffinity getAffinity() {
        return SQLite3TypeAffinity.NONE;
    }

    /*
     * See https://www.sqlite.org/datatype3.html#assigning_collating_sequences_from_sql 7.1
     *
     */

    default SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

    default SQLite3CollateSequence getImplicitCollateSequence() {
        return null;
    }

}
