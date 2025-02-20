package sqlancer.postgres.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresJoin extends JoinBase<PostgresExpression>
        implements PostgresExpression, Join<PostgresExpression, PostgresTable, PostgresColumn> {

    public PostgresJoin(PostgresExpression tableReference, PostgresExpression onClause, JoinType type) {
        super(tableReference, onClause, type);
    }

    public PostgresJoin(PostgresExpression leftTable, PostgresExpression rightTable, JoinType joinType,
            PostgresExpression whereCondition) {
        super(null, whereCondition, joinType, leftTable, rightTable);
    }

    public static PostgresJoin createJoin(PostgresExpression left, PostgresExpression right, JoinType type,
            PostgresExpression onClause) {
        if (type == JoinType.CROSS) {
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
            joinExpressions.add(PostgresJoin.createJoin(left, right, JoinType.getRandom(),
                    joinGen.generateExpression(0, PostgresDataType.BOOLEAN)));
        }
        return joinExpressions;
    }

    @Override
    public void setOnClause(PostgresExpression clause) {
        this.onClause = clause;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    @Override
    public PostgresExpression getTableReference() {
        return tableReference;
    }

    public PostgresExpression getLeftTable() {
        return leftTable;
    }

    public PostgresExpression getRightTable() {
        return rightTable;
    }

    @Override
    public PostgresExpression getOnClause() {
        return onClause;
    }

    @Override
    public JoinType getType() {
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
