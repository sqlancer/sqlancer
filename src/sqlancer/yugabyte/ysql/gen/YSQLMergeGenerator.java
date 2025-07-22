package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;

public final class YSQLMergeGenerator {

    private YSQLMergeGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        YSQLTable targetTable = globalState.getSchema().getRandomTable(t -> !t.isView());
        return create(targetTable, globalState);
    }

    public static SQLQueryAdapter create(YSQLTable targetTable, YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("MERGE INTO ");
        sb.append(targetTable.getName());
        sb.append(" AS target");
        
        // Add source - either a table or VALUES clause
        sb.append(" USING ");
        
        boolean useValuesClause = Randomly.getBoolean();
        if (useValuesClause || globalState.getSchema().getDatabaseTables().size() == 1) {
            // Use VALUES clause
            sb.append("(VALUES ");
            int numRows = Randomly.smallNumber() + 1;
            for (int i = 0; i < numRows; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("(");
                List<YSQLColumn> columns = targetTable.getColumns();
                for (int j = 0; j < columns.size(); j++) {
                    if (j > 0) {
                        sb.append(", ");
                    }
                    YSQLExpression expr = YSQLExpressionGenerator.generateConstant(globalState.getRandomly(), 
                                                                                   columns.get(j).getType());
                    sb.append(YSQLVisitor.asString(expr));
                }
                sb.append(")");
            }
            sb.append(") AS source(");
            for (int i = 0; i < targetTable.getColumns().size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("c").append(i);
            }
            sb.append(")");
        } else {
            // Use another table
            YSQLTable sourceTable = globalState.getSchema().getRandomTable(t -> !t.isView() && t != targetTable);
            sb.append(sourceTable.getName());
            sb.append(" AS source");
        }
        
        // ON clause
        sb.append(" ON ");
        YSQLExpression joinCondition = YSQLExpressionGenerator.generateExpression(globalState, YSQLDataType.BOOLEAN);
        sb.append(YSQLVisitor.asString(joinCondition));
        
        // WHEN clauses - ensure at least one WHEN clause is present
        boolean hasWhenMatched = Randomly.getBoolean();
        boolean hasWhenNotMatchedByTarget = Randomly.getBoolean();
        // WHEN NOT MATCHED BY SOURCE is PostgreSQL 15 feature, not in YugabyteDB yet
        boolean hasWhenNotMatchedBySource = false;
        
        // If no WHEN clause was selected, force at least one
        if (!hasWhenMatched && !hasWhenNotMatchedByTarget) {
            // Randomly select one to be true
            if (Randomly.getBoolean()) {
                hasWhenMatched = true;
            } else {
                hasWhenNotMatchedByTarget = true;
            }
        }
        
        if (hasWhenMatched) {
            sb.append(" WHEN MATCHED");
            if (Randomly.getBoolean()) {
                sb.append(" AND ");
                YSQLExpression condition = YSQLExpressionGenerator.generateExpression(globalState, YSQLDataType.BOOLEAN);
                sb.append(YSQLVisitor.asString(condition));
            }
            sb.append(" THEN ");
            
            if (Randomly.getBoolean()) {
                // UPDATE
                sb.append("UPDATE SET ");
                List<YSQLColumn> columns = targetTable.getRandomNonEmptyColumnSubset();
                for (int i = 0; i < columns.size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    YSQLColumn column = columns.get(i);
                    sb.append(column.getName());
                    sb.append(" = ");
                    if (Randomly.getBoolean() && !useValuesClause) {
                        // Only reference source columns when using a table, not VALUES
                        sb.append("source.");
                        sb.append(column.getName());
                    } else {
                        YSQLExpression expr = YSQLExpressionGenerator.generateExpression(globalState, column.getType());
                        sb.append(YSQLVisitor.asString(expr));
                    }
                }
            } else {
                // DELETE
                sb.append("DELETE");
            }
        }
        
        if (hasWhenNotMatchedByTarget) {
            sb.append(" WHEN NOT MATCHED");
            if (Randomly.getBoolean()) {
                sb.append(" AND ");
                YSQLExpression condition = YSQLExpressionGenerator.generateExpression(globalState, YSQLDataType.BOOLEAN);
                sb.append(YSQLVisitor.asString(condition));
            }
            sb.append(" THEN INSERT");
            
            List<YSQLColumn> columns = Randomly.getBoolean() ? targetTable.getColumns() : targetTable.getRandomNonEmptyColumnSubset();
            if (columns.size() < targetTable.getColumns().size()) {
                sb.append(" (");
                sb.append(columns.stream().map(YSQLColumn::getName).collect(Collectors.joining(", ")));
                sb.append(")");
            }
            
            sb.append(" VALUES (");
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                if (Randomly.getBoolean() && !useValuesClause) {
                    sb.append("source.");
                    sb.append(columns.get(i).getName());
                } else {
                    YSQLExpression expr = YSQLExpressionGenerator.generateExpression(globalState, columns.get(i).getType());
                    sb.append(YSQLVisitor.asString(expr));
                }
            }
            sb.append(")");
        }
        
        if (hasWhenNotMatchedBySource) {
            sb.append(" WHEN NOT MATCHED BY SOURCE");
            if (Randomly.getBoolean()) {
                sb.append(" AND ");
                YSQLExpression condition = YSQLExpressionGenerator.generateExpression(globalState, YSQLDataType.BOOLEAN);
                sb.append(YSQLVisitor.asString(condition));
            }
            sb.append(" THEN ");
            
            if (Randomly.getBoolean()) {
                // UPDATE
                sb.append("UPDATE SET ");
                YSQLColumn column = targetTable.getRandomColumn();
                sb.append(column.getName());
                sb.append(" = ");
                YSQLExpression expr = YSQLExpressionGenerator.generateExpression(globalState, column.getType());
                sb.append(YSQLVisitor.asString(expr));
            } else {
                // DELETE
                sb.append("DELETE");
            }
        }
        
        // Add common MERGE errors
        errors.add("MERGE is not supported");
        errors.add("MERGE command cannot affect row a second time");
        errors.add("MERGE is not supported on tables with rules");
        errors.add("MERGE is not supported on foreign tables");
        errors.add("MERGE is not supported on partitioned tables");
        errors.add("MERGE is not supported for tables with inheritance");
        errors.add("multiple actions for MERGE");
        errors.add("invalid reference to FROM-clause entry for table");
        errors.add("column used in WHEN AND condition must appear in USING clause");
        errors.add("target row matched more than once");
        errors.add("WHEN NOT MATCHED BY SOURCE is not supported");
        
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonInsertUpdateErrors(errors);
        
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}