package sqlancer.mysql.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public class MySQLJoin implements MySQLExpression {

    public enum JoinType {
        NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS;
    }

    private final MySQLTable table;
    private MySQLExpression onClause;
    private JoinType type;

    public MySQLJoin(MySQLJoin other) {
        this.table = other.table;
        this.onClause = other.onClause;
        this.type = other.type;
    }

    public MySQLJoin(MySQLTable table, MySQLExpression onClause, JoinType type) {
        this.table = table;
        this.onClause = onClause;
        this.type = type;
    }

    public MySQLTable getTable() {
        return table;
    }

    public MySQLExpression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
        return type;
    }

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
