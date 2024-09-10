package sqlancer.oceanbase.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.oceanbase.OceanBaseErrors;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.ast.OceanBaseExpression;
import sqlancer.oceanbase.ast.OceanBaseJoin;
import sqlancer.oceanbase.ast.OceanBaseSelect;
import sqlancer.oceanbase.gen.OceanBaseExpressionGenerator;

public class OceanBaseNoRECOracle implements TestOracle<OceanBaseGlobalState> {

    NoRECOracle<OceanBaseSelect, OceanBaseJoin, OceanBaseExpression, OceanBaseSchema, OceanBaseTable, OceanBaseColumn, OceanBaseGlobalState> oracle;

    public OceanBaseNoRECOracle(OceanBaseGlobalState globalState) {
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(OceanBaseErrors.getExpressionErrors())
                .withRegex(OceanBaseErrors.getExpressionErrorsRegex())
                .with("canceling statement due to statement timeout").with("unmatched parentheses")
                .with("nothing to repeat at offset").with("missing )").with("missing terminating ]")
                .with("range out of order in character class").with("unrecognized character after ")
                .with("Got error '(*VERB) not recognized or malformed").with("must be followed by")
                .with("malformed number or name after").with("digit expected after").build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<OceanBaseGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
