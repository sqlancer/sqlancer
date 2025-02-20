package sqlancer.mysql.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public class MySQLJoin extends JoinBase<MySQLExpression>
        implements MySQLExpression, Join<MySQLExpression, MySQLTable, MySQLColumn> {

    private final MySQLTable table;

    public MySQLJoin(MySQLJoin other) {
        super(null, other.onClause, other.type);
        this.table = other.table;
    }

    public MySQLJoin(MySQLTable table, MySQLExpression onClause, JoinType type) {
        super(null, onClause, type);
        this.table = table;
    }

    public MySQLTable getTable() {
        return table;
    }

    @Override
    public MySQLExpression getOnClause() {
        return onClause;
    }

    @Override
    public JoinType getType() {
        return type;
    }

    @Override
    public void setOnClause(MySQLExpression onClause) {
        this.onClause = onClause;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public static List<MySQLJoin> getRandomJoinClauses(List<MySQLTable> tables, MySQLGlobalState globalState) {
        List<MySQLJoin> joinStatements = new ArrayList<>();
        List<JoinType> options = new ArrayList<>(Arrays.asList(JoinType.values()));
        List<MySQLColumn> columns = new ArrayList<>();
        if (tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1) {
                options.remove(JoinType.NATURAL);
            }
            for (int i = 0; i < nrJoinClauses; i++) {
                MySQLTable table = Randomly.fromList(tables);
                tables.remove(table);
                columns.addAll(table.getColumns());
                MySQLExpressionGenerator joinGen = new MySQLExpressionGenerator(globalState).setColumns(columns);
                MySQLExpression joinClause = joinGen.generateExpression();
                JoinType selectedOption = Randomly.fromList(options);
                if (selectedOption == JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    joinClause = null;
                }
                MySQLJoin j = new MySQLJoin(table, joinClause, selectedOption);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }
}
