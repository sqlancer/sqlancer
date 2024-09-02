package sqlancer.cockroachdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

public class CockroachDBNoRECOracle implements TestOracle<CockroachDBGlobalState> {

    NoRECOracle<CockroachDBSelect, CockroachDBJoin, CockroachDBExpression, CockroachDBSchema, CockroachDBTable, CockroachDBColumn, CockroachDBGlobalState> oracle;

    public CockroachDBNoRECOracle(CockroachDBGlobalState globalState) {
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState);
        ExpectedErrors errors = ExpectedErrors.newErrors().with(CockroachDBErrors.getExpressionErrors())
                .with(CockroachDBErrors.getTransactionErrors()).with("unable to vectorize execution plan") // SET
                                                                                                           // vectorize=experimental_always;
                .with(" mismatched physical types at index") // SET vectorize=experimental_always;
                .build();
        this.oracle = new NoRECOracle<>(globalState, gen, errors);
    }

    @Override
    public void check() throws SQLException {
        oracle.check();
    }

    @Override
    public Reproducer<CockroachDBGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }

    public static List<CockroachDBExpression> getJoins(List<CockroachDBExpression> tableList,
            CockroachDBGlobalState globalState) throws AssertionError {
        List<CockroachDBExpression> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBoolean()) {
            CockroachDBTableReference leftTable = (CockroachDBTableReference) tableList.remove(0);
            CockroachDBTableReference rightTable = (CockroachDBTableReference) tableList.remove(0);
            List<CockroachDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            CockroachDBExpressionGenerator joinGen = new CockroachDBExpressionGenerator(globalState)
                    .setColumns(columns);
            joinExpressions.add(CockroachDBJoin.createJoin(leftTable, rightTable, CockroachDBJoin.JoinType.getRandom(),
                    joinGen.generateExpression(CockroachDBDataType.BOOL.get())));
        }
        return joinExpressions;
    }

}
