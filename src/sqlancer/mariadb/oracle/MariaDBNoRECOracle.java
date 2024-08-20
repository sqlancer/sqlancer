package sqlancer.mariadb.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBJoin;
import sqlancer.mariadb.ast.MariaDBSelectStatement;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;

public class MariaDBNoRECOracle implements TestOracle<MariaDBGlobalState> {

    NoRECOracle<MariaDBSelectStatement, MariaDBJoin, MariaDBExpression, MariaDBSchema, MariaDBTable, MariaDBColumn, MariaDBGlobalState> oracle;

    public MariaDBNoRECOracle(MariaDBGlobalState globalState) {
        MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(globalState.getRandomly());
        ExpectedErrors errors = ExpectedErrors.newErrors().with("is out of range").with("unmatched parentheses")
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
    public Reproducer<MariaDBGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }
}
