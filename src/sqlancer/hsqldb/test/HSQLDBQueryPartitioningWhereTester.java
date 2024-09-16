package sqlancer.hsqldb.test;

import java.sql.SQLException;

import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.hsqldb.HSQLDBErrors;
import sqlancer.hsqldb.HSQLDBProvider.HSQLDBGlobalState;
import sqlancer.hsqldb.HSQLDBSchema;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBColumn;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBTable;
import sqlancer.hsqldb.ast.HSQLDBExpression;
import sqlancer.hsqldb.ast.HSQLDBJoin;
import sqlancer.hsqldb.ast.HSQLDBSelect;
import sqlancer.hsqldb.gen.HSQLDBExpressionGenerator;

public class HSQLDBQueryPartitioningWhereTester implements TestOracle<HSQLDBGlobalState> {

    private final TLPWhereOracle<HSQLDBSelect, HSQLDBJoin, HSQLDBExpression, HSQLDBSchema, HSQLDBTable, HSQLDBColumn, HSQLDBGlobalState> oracle;

    public HSQLDBQueryPartitioningWhereTester(HSQLDBGlobalState state) {
        HSQLDBExpressionGenerator gen = new HSQLDBExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(HSQLDBErrors.getExpressionErrors()).build();

        this.oracle = new TLPWhereOracle<>(state, gen, expectedErrors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
