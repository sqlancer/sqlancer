package sqlancer.mariadb.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;

public class MariaDBJoin implements MariaDBExpression {

    public enum JoinType {
        NATURAL, INNER, STRAIGHT, LEFT, RIGHT, CROSS;
    }

    private final MariaDBTable table;
    private MariaDBExpression onClause;
    private JoinType type;

    public MariaDBJoin(MariaDBJoin other) {
        this.table = other.table;
        this.onClause = other.onClause;
        this.type = other.type;
    }

    public MariaDBJoin(MariaDBTable table, MariaDBExpression onClause, JoinType type) {
        this.table = table;
        this.onClause = onClause;
        this.type = type;
    }

    public MariaDBTable getTable() {
        return table;
    }

    public MariaDBExpression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
        return type;
    }

    public void setOnClause(MariaDBExpression onClause) {
        this.onClause = onClause;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public static List<MariaDBJoin> getRandomJoinClauses(List<MariaDBTable> tables, MariaDBGlobalState globalState) {
        List<MariaDBJoin> joinStatements = new ArrayList<>();
        List<JoinType> options = new ArrayList<>(Arrays.asList(JoinType.values()));
        List<MariaDBColumn> columns = new ArrayList<>();
        if (tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1) {
                options.remove(JoinType.NATURAL);
            }
            for (int i = 0; i < nrJoinClauses; i++) {
                MariaDBTable table = Randomly.fromList(tables);
                tables.remove(table);
                columns.addAll(table.getColumns());
                MariaDBExpressionGenerator joinGen = new MariaDBExpressionGenerator(globalState.getRandomly())
                        .setColumns(columns);
                MariaDBExpression joinClause = joinGen.getRandomExpression();
                JoinType selectedOption = Randomly.fromList(options);
                if (selectedOption == JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    joinClause = null;
                }
                MariaDBJoin j = new MariaDBJoin(table, joinClause, selectedOption);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }
}
