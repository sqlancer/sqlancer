package sqlancer.yugabyte.ysql;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase.JoinType;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLJoin;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.gen.YSQLExpressionGenerator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTables;

/**
 * Utility class for YSQL-related operations.
 */
public final class YSQLUtils {

    private YSQLUtils() {
    }

    public static List<YSQLJoin> getJoinStatements(YSQLGlobalState globalState, List<YSQLSchema.YSQLColumn> columns,
            List<YSQLTable> tables) {
        List<YSQLJoin> joinStatements = new ArrayList<>();
        YSQLExpressionGenerator gen = new YSQLExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            YSQLExpression joinClause = gen.generateExpression(YSQLDataType.BOOLEAN);
            YSQLTable table = Randomly.fromList(tables);
            tables.remove(table);
            JoinType options = JoinType.getRandomForDatabase("YSQL");
            YSQLJoin j = new YSQLJoin(new YSQLSelect.YSQLFromTable(table, Randomly.getBoolean()), joinClause, options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            YSQLTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            YSQLSelect.YSQLSubquery subquery = YSQLExpressionGenerator.createSubquery(globalState,
                    String.format("sub%d", i), subqueryTables);
            YSQLExpression joinClause = gen.generateExpression(YSQLDataType.BOOLEAN);
            JoinType options = JoinType.getRandomForDatabase("YSQL");
            YSQLJoin j = new YSQLJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }
}
