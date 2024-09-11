package sqlancer.h2;

import java.sql.SQLException;

import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2Table;
import sqlancer.h2.ast.H2Expression;
import sqlancer.h2.ast.H2Join;
import sqlancer.h2.ast.H2Select;

public class H2QueryPartitioningWhereTester implements TestOracle<H2GlobalState> {

    private final TLPWhereOracle<H2Select, H2Join, H2Expression, H2Schema, H2Table, H2Column, H2GlobalState> oracle;

    public H2QueryPartitioningWhereTester(H2GlobalState state) {
        H2ExpressionGenerator gen = new H2ExpressionGenerator(state);
        ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(H2Errors.getExpressionErrors()).build();

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
