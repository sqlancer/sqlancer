package sqlancer.tidb.gen;

import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.visitor.TiDBVisitor;

public final class TiDBDeleteGenerator extends AbstractDeleteGenerator {

    private final TiDBGlobalState globalState;

    private TiDBDeleteGenerator(TiDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) {
        return new TiDBDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        errors.addAll(TiDBErrors.getExpressionErrors());
        TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("DELETE ");
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append("LOW_PRIORITY ");
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append("QUICK ");
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append("IGNORE ");
        }
        sb.append("FROM ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            appendWhereClause(TiDBVisitor.asString(gen.generateExpression()));
            errors.add("Truncated incorrect");
            errors.add("Data truncation");
            errors.add("Truncated incorrect FLOAT value");
        }
        if (Randomly.getBoolean()) {
            sb.append(" ORDER BY ");
            TiDBErrors.addExpressionErrors(errors);
            sb.append(gen.generateOrderBys().stream().map(o -> TiDBVisitor.asString(o))
                    .collect(Collectors.joining(", ")));
        }
        if (Randomly.getBoolean()) {
            appendLimitClause(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE));
        }
        errors.add("Bad Number");
        errors.add("Truncated incorrect"); // https://github.com/pingcap/tidb/issues/24292
        errors.add("is not valid for CHARACTER SET");
        errors.add("Division by 0");
        errors.add("error parsing regexp");
    }

}
