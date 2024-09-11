package sqlancer.hsqldb.test;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
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

public class HSQLDBNoRECOracle implements TestOracle<HSQLDBGlobalState> {

    NoRECOracle<HSQLDBSelect, HSQLDBJoin, HSQLDBExpression, HSQLDBSchema, HSQLDBTable, HSQLDBColumn, HSQLDBGlobalState> oracle;

    public HSQLDBNoRECOracle(HSQLDBGlobalState globalState) {
        HSQLDBExpressionGenerator gen = new HSQLDBExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(HSQLDBErrors.getExpressionErrors()).build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<HSQLDBGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
