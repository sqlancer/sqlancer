package sqlancer.materialize;

import static sqlancer.materialize.oracle.tlp.MaterializeTLPBase.createSubquery;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.JoinBase.JoinType;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeSchema.MaterializeTables;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeJoin;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeFromTable;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeSubquery;
import sqlancer.materialize.gen.MaterializeExpressionGenerator;

public final class MaterializeUtils {

    private MaterializeUtils() {
    }

    public static List<MaterializeJoin> getJoinStatements(MaterializeGlobalState globalState,
            List<MaterializeColumn> columns, List<MaterializeTable> tables) {
        List<MaterializeJoin> joinStatements = new ArrayList<>();
        MaterializeExpressionGenerator gen = new MaterializeExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            MaterializeExpression joinClause = gen.generateExpression(MaterializeDataType.BOOLEAN);
            MaterializeTable table = Randomly.fromList(tables);
            tables.remove(table);
            JoinBase.JoinType options = JoinBase.JoinType.getRandom();
            MaterializeJoin j = new MaterializeJoin(new MaterializeFromTable(table, Randomly.getBoolean()), joinClause,
                    options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            MaterializeTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            MaterializeSubquery subquery = createSubquery(globalState, String.format("sub%d", i), subqueryTables);
            MaterializeExpression joinClause = gen.generateExpression(MaterializeDataType.BOOLEAN);
            JoinType options = JoinType.getRandom();
            MaterializeJoin j = new MaterializeJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }

        return joinStatements;
    }
}
