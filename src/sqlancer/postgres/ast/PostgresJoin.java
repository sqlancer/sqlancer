package sqlancer.postgres.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresJoin implements PostgresExpression {

    public enum PostgresJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;

        public static PostgresJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static PostgresJoinType getRandomExcept(PostgresJoinType... exclude) {
            PostgresJoinType[] values = Arrays.stream(values()).filter(m -> !Arrays.asList(exclude).contains(m))
                    .toArray(PostgresJoinType[]::new);
            return Randomly.fromOptions(values);
        }

    }

    private final PostgresExpression tableReference;
    private PostgresExpression onClause;
    private PostgresJoinType type;
    private final PostgresExpression leftTable;
    private final PostgresExpression rightTable;

    public PostgresJoin(PostgresExpression tableReference, PostgresExpression onClause, PostgresJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
        this.leftTable = null;
        this.rightTable = null;
    }

    public PostgresJoin(PostgresExpression leftTable, PostgresExpression rightTable, PostgresJoinType joinType,
            PostgresExpression whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.type = joinType;
        this.onClause = whereCondition;
        this.tableReference = null;
    }

    public static PostgresJoin createJoin(PostgresExpression left, PostgresExpression right, PostgresJoinType type,
            PostgresExpression onClause) {
        if (type == PostgresJoinType.CROSS) {
            return new PostgresJoin(left, right, type, null);
        } else {
            return new PostgresJoin(left, right, type, onClause);
        }
    }

    public static List<PostgresExpression> getJoins(List<PostgresExpression> tableList,
            PostgresGlobalState globalState) {
        // Clone Table to prevent the original list from being manipulated
        List<PostgresExpression> tbl = new ArrayList<>(tableList);
        List<PostgresExpression> joinExpressions = new ArrayList<>();
        while (tbl.size() >= 2 && Randomly.getBoolean()) {
            PostgresTableReference left = (PostgresTableReference) tbl.remove(0);
            PostgresTableReference right = (PostgresTableReference) tbl.remove(0);
            List<PostgresColumn> columns = new ArrayList<>();
            columns.addAll(left.getTable().getColumns());
            columns.addAll(right.getTable().getColumns());
            PostgresExpressionGenerator joinGen = new PostgresExpressionGenerator(globalState).setColumns(columns);
            joinExpressions.add(PostgresJoin.createJoin(left, right, PostgresJoinType.getRandom(),
                    joinGen.generateExpression(0, PostgresDataType.BOOLEAN)));
        }
        return joinExpressions;
    }

    public void setOnClause(PostgresExpression clause) {
        this.onClause = clause;
    }

    public void setType(PostgresJoinType type) {
        this.type = type;
    }

    public PostgresExpression getTableReference() {
        return tableReference;
    }

    public PostgresExpression getLeftTable() {
        return leftTable;
    }

    public PostgresExpression getRightTable() {
        return rightTable;
    }

    public PostgresExpression getOnClause() {
        return onClause;
    }

    public PostgresJoinType getType() {
        return type;
    }

    @Override
    public PostgresDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public PostgresConstant getExpectedValue() {
        throw new AssertionError();
    }

}
