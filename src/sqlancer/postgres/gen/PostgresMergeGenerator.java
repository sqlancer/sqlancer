package sqlancer.postgres.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;

public final class PostgresMergeGenerator {

    private PostgresMergeGenerator() {
    }

    public static SQLQueryAdapter merge(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        return mergeInto(globalState, table);
    }

    public static SQLQueryAdapter mergeInto(PostgresGlobalState globalState, PostgresTable targetTable) {
        ExpectedErrors errors = new ExpectedErrors();

        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonInsertUpdateErrors(errors);
        errors.add("multiple assignments to same column");
        errors.add("violates foreign key constraint");
        errors.add("value too long for type character varying");
        errors.add("conflicting key value violates exclusion constraint");
        errors.add("violates not-null constraint");
        errors.add("current transaction is aborted");
        errors.add("bit string too long");
        errors.add("new row violates check option for view");
        errors.add("reached maximum value of sequence");
        errors.add("but expression is of type");
        errors.add("duplicate key value violates unique constraint");
        errors.add("identity column defined as GENERATED ALWAYS");
        errors.add("out of range");
        errors.add("violates check constraint");
        errors.add("no partition of relation");
        errors.add("division by zero");
        errors.add("data type unknown");
        errors.add("MERGE statement requires a source table");
        errors.add("MERGE statement requires a matching condition");
        errors.add("MERGE statement requires at least one action");
        errors.add("column reference is ambiguous");
        errors.add("table name specified more than once");
        errors.add("cannot reference target table in source");
        errors.add("MERGE target table must be updatable");
        errors.add("MERGE source must be a table or subquery");
        errors.add("MERGE condition must be a boolean expression");
        errors.add("MERGE action must be INSERT, UPDATE, or DELETE");
        errors.add("MERGE cannot be used with views");
        errors.add("MERGE cannot be used with temporary tables");
        errors.add("MERGE requires appropriate privileges");
        errors.add("MERGE statement is not supported in this context");
        errors.add("MERGE cannot be used in a function");
        errors.add("MERGE cannot be used in a trigger");
        errors.add("MERGE cannot be used in a stored procedure");
        errors.add("MERGE statement is not allowed in this transaction mode");
        errors.add("invalid reference to FROM-clause entry for table");
        errors.add("column does not exist");
        errors.add("relation does not exist");
        errors.add("permission denied");
        errors.add("cannot insert into column");
        errors.add("cannot update column");
        errors.add("ERROR: MERGE command cannot affect row a second time");
        errors.add("ERROR: no collation was derived for column \".*\" with collatable type text");

        try {
            StringBuilder sb = new StringBuilder();

            sb.append("MERGE INTO ");
            sb.append(targetTable.getName());
            sb.append(" AS target");

            PostgresTable sourceTable = getSourceTable(globalState, targetTable);
            List<PostgresColumn> sourceColumns = null;
            boolean useSubquery = false;
            if (sourceTable != null) {
                sb.append(" USING ");
                sb.append(sourceTable.getName());
                sb.append(" AS source");
            } else {
                useSubquery = true;
                sourceColumns = targetTable.getRandomNonEmptyColumnSubset();
                sb.append(" USING (");
                sb.append(generateSourceSubquery(globalState, targetTable, sourceColumns));
                sb.append(") AS source");
            }

            sb.append(" ON ");
            if (useSubquery) {
                sb.append(generateMatchingCondition(targetTable, sourceColumns));
            } else {
                sb.append(generateMatchingCondition(targetTable, sourceTable));
            }

            boolean hasMatchedClause = false;
            boolean hasNotMatchedClause = false;

            if (Randomly.getBoolean()) {
                sb.append(" WHEN MATCHED THEN");
                hasMatchedClause = true;
                if (Randomly.getBoolean()) {
                    sb.append(" UPDATE SET ");
                    try {
                        if (useSubquery) {
                            sb.append(generateUpdateSet(targetTable, sourceColumns));
                        } else {
                            sb.append(generateUpdateSet(targetTable, sourceTable));
                        }
                    } catch (Exception e) {
                        PostgresColumn column = targetTable.getRandomColumn();
                        sb.append(column.getName()).append(" = 0");
                    }

                    if (Randomly.getBooleanWithSmallProbability() && Randomly.getBooleanWithSmallProbability()) {
                        sb.append(" WHERE ");
                        sb.append(generateWhereCondition(targetTable));
                    }
                } else {
                    sb.append(" DELETE");
                    if (Randomly.getBooleanWithSmallProbability() && Randomly.getBooleanWithSmallProbability()) {
                        sb.append(" WHERE ");
                        sb.append(generateWhereCondition(targetTable));
                    }
                }
            }

            if (Randomly.getBoolean()) {
                sb.append(" WHEN NOT MATCHED THEN");
                hasNotMatchedClause = true;
                sb.append(" INSERT (");
                List<PostgresColumn> insertColumns = targetTable.getRandomNonEmptyColumnSubset();
                sb.append(insertColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
                sb.append(") VALUES (");
                try {
                    if (useSubquery) {
                        sb.append(generateInsertValues(insertColumns, sourceColumns));
                    } else {
                        sb.append(generateInsertValues(insertColumns, sourceTable));
                    }
                } catch (Exception e) {
                    sb.append(insertColumns.stream().map(c -> "0").collect(Collectors.joining(", ")));
                }
                sb.append(")");

                if (!sb.toString().endsWith(")")) {
                    sb.append(")");
                }
            }

            if (!hasMatchedClause && !hasNotMatchedClause) {
                sb.append(" WHEN MATCHED THEN UPDATE SET ");
                PostgresColumn column = targetTable.getRandomColumn();
                sb.append(column.getName());
                sb.append(" = ");

                try {
                    boolean sourceHasColumn;
                    if (useSubquery) {
                        sourceHasColumn = sourceHasColumn(column, sourceColumns);
                    } else {
                        sourceHasColumn = sourceHasColumn(column, sourceTable);
                    }

                    if (sourceHasColumn) {
                        sb.append(generateColumnReference(column, "source"));
                    } else {
                        sb.append("0");
                    }
                } catch (Exception e) {
                    sb.append("0");
                }
            }

            String finalStatement = sb.toString();
            if (!finalStatement.contains("WHEN MATCHED") && !finalStatement.contains("WHEN NOT MATCHED")) {
                finalStatement += " WHEN MATCHED THEN UPDATE SET c0 = 0";
            }

            return new SQLQueryAdapter(finalStatement, errors);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append("MERGE INTO ");
            sb.append(targetTable.getName());
            sb.append(" AS target USING ");
            sb.append(targetTable.getName());
            sb.append(" AS source ON target.c0 = source.c0 WHEN MATCHED THEN UPDATE SET c0 = source.c0");
            return new SQLQueryAdapter(sb.toString(), errors);
        }
    }

    // Helper method to generate a column reference with table prefix
    private static String generateColumnReference(PostgresColumn column, String tablePrefix) {
        return tablePrefix + "." + column.getName();
    }

    // Helper method to check if source has a matching column
    private static boolean sourceHasColumn(PostgresColumn targetColumn, List<PostgresColumn> sourceColumns) {
        return sourceColumns.stream().anyMatch(c -> c.getName().equals(targetColumn.getName()));
    }

    // Helper method to check if source table has a matching column
    private static boolean sourceHasColumn(PostgresColumn targetColumn, PostgresTable sourceTable) {
        return sourceTable != null
                && sourceTable.getColumns().stream().anyMatch(c -> c.getName().equals(targetColumn.getName()));
    }

    private static PostgresTable getSourceTable(PostgresGlobalState globalState, PostgresTable targetTable) {
        List<PostgresTable> availableTables = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> !t.getName().equals(targetTable.getName()) && t.isInsertable())
                .collect(Collectors.toList());

        if (availableTables.isEmpty()) {
            return null;
        }

        try {
            return Randomly.fromList(availableTables);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    private static String generateSourceSubquery(PostgresGlobalState globalState, PostgresTable targetTable,
            List<PostgresColumn> sourceColumns) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        sb.append(sourceColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));

        sb.append(" FROM ");

        PostgresTable sourceTable = null;
        try {
            sourceTable = globalState.getSchema()
                    .getRandomTable(t -> !t.getName().equals(targetTable.getName()) && t.isInsertable());
        } catch (IndexOutOfBoundsException e) {

            sourceTable = targetTable;
        }

        if (sourceTable == null) {
            sourceTable = targetTable;
        }

        sb.append(sourceTable.getName());
        sb.append(" source");

        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState)
                    .setColumns(sourceTable.getColumns());
            sb.append(PostgresVisitor.asString(gen.generateExpression(0, PostgresSchema.PostgresDataType.BOOLEAN)));
        }

        return sb.toString();
    }

    private static String generateMatchingCondition(PostgresTable targetTable, List<PostgresColumn> sourceColumns) {
        StringBuilder sb = new StringBuilder();

        List<PostgresColumn> commonColumns = targetTable.getColumns().stream()
                .filter(targetCol -> sourceHasColumn(targetCol, sourceColumns)).collect(Collectors.toList());

        if (commonColumns.isEmpty()) {
            PostgresColumn column = targetTable.getRandomColumn();
            sb.append(generateColumnReference(column, "target"));
            sb.append(" IS NOT NULL");
            return sb.toString();
        }

        List<PostgresColumn> columns = Randomly.nonEmptySubset(commonColumns);
        if (columns.size() > 1) {
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sb.append(" AND ");
                }
                PostgresColumn column = columns.get(i);
                sb.append(generateColumnReference(column, "target"));
                sb.append(" = ");
                sb.append(generateColumnReference(column, "source"));
            }
        } else {
            PostgresColumn column = columns.get(0);
            sb.append(generateColumnReference(column, "target"));
            sb.append(" = ");
            sb.append(generateColumnReference(column, "source"));
        }

        return sb.toString();
    }

    private static String generateUpdateSet(PostgresTable targetTable, List<PostgresColumn> sourceColumns) {
        StringBuilder sb = new StringBuilder();

        List<PostgresColumn> updateColumns = targetTable.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < updateColumns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            PostgresColumn column = updateColumns.get(i);
            sb.append(column.getName());
            sb.append(" = ");

            if (Randomly.getBoolean() && sourceHasColumn(column, sourceColumns)) {
                sb.append(generateColumnReference(column, "source"));
            } else {
                sb.append("0");
            }
        }

        return sb.toString();
    }

    private static String generateInsertValues(List<PostgresColumn> columns, List<PostgresColumn> sourceColumns) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }

            PostgresColumn column = columns.get(i);

            if (Randomly.getBoolean() && sourceHasColumn(column, sourceColumns)) {
                sb.append(generateColumnReference(column, "source"));
            } else {
                sb.append("0");
            }
        }

        return sb.toString();
    }

    private static String generateWhereCondition(PostgresTable targetTable) {
        if (Randomly.getBoolean()) {
            PostgresColumn column = targetTable.getRandomColumn();
            return generateColumnReference(column, "target") + " IS NOT NULL";
        } else {
            return "TRUE";
        }
    }

    private static String generateMatchingCondition(PostgresTable targetTable, PostgresTable sourceTable) {
        StringBuilder sb = new StringBuilder();

        List<PostgresColumn> commonColumns = targetTable.getColumns().stream()
                .filter(targetCol -> sourceHasColumn(targetCol, sourceTable)).collect(Collectors.toList());

        if (commonColumns.isEmpty()) {
            PostgresColumn column = targetTable.getRandomColumn();
            sb.append(generateColumnReference(column, "target"));
            sb.append(" IS NOT NULL");
            return sb.toString();
        }

        List<PostgresColumn> columns = Randomly.nonEmptySubset(commonColumns);
        if (columns.size() > 1) {
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sb.append(" AND ");
                }
                PostgresColumn column = columns.get(i);
                sb.append(generateColumnReference(column, "target"));
                sb.append(" = ");
                sb.append(generateColumnReference(column, "source"));
            }
        } else {
            PostgresColumn column = columns.get(0);
            sb.append(generateColumnReference(column, "target"));
            sb.append(" = ");
            sb.append(generateColumnReference(column, "source"));
        }

        return sb.toString();
    }

    private static String generateUpdateSet(PostgresTable targetTable, PostgresTable sourceTable) {
        StringBuilder sb = new StringBuilder();

        List<PostgresColumn> updateColumns = targetTable.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < updateColumns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            PostgresColumn column = updateColumns.get(i);
            sb.append(column.getName());
            sb.append(" = ");

            if (Randomly.getBoolean() && sourceHasColumn(column, sourceTable)) {
                sb.append(generateColumnReference(column, "source"));
            } else {
                sb.append("0");
            }
        }

        return sb.toString();
    }

    private static String generateInsertValues(List<PostgresColumn> columns, PostgresTable sourceTable) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }

            PostgresColumn column = columns.get(i);

            if (Randomly.getBoolean() && sourceHasColumn(column, sourceTable)) {
                sb.append(generateColumnReference(column, "source"));
            } else {
                sb.append("0");
            }
        }

        return sb.toString();
    }
}
