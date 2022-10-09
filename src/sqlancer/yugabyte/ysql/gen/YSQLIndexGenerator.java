package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLIndex;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;

public final class YSQLIndexGenerator {

    private YSQLIndexGenerator() {
    }

    public static SQLQueryAdapter generate(YSQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (Randomly.getBoolean()) {
            sb.append(" UNIQUE");
        }
        sb.append(" INDEX ");
        YSQLTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView()); // TODO: materialized
        // views
        String indexName = getNewIndexName(randomTable);
        sb.append(indexName);
        sb.append(" ON ");
        if (Randomly.getBoolean()) {
            sb.append("ONLY ");
        }
        sb.append(randomTable.getName());
        IndexType method;
        if (Randomly.getBoolean()) {
            sb.append(" USING ");
            method = Randomly.fromOptions(IndexType.values());
            sb.append(method);
        } else {
            method = IndexType.BTREE;
        }

        sb.append("(");
        if (method == IndexType.HASH) {
            sb.append(randomTable.getRandomColumn().getName());
        } else {
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                if (Randomly.getBoolean()) {
                    sb.append(randomTable.getRandomColumn().getName());
                } else {
                    sb.append("(");
                    YSQLExpression expression = YSQLExpressionGenerator.generateExpression(globalState,
                            randomTable.getColumns());
                    sb.append(YSQLVisitor.asString(expression));
                    sb.append(")");
                }

                if (Randomly.getBooleanWithRatherLowProbability()) {
                    sb.append(" ");
                    sb.append(globalState.getRandomOpclass());
                    errors.add("does not accept");
                    errors.add("does not exist for access method");
                }
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("ASC", "DESC"));
                }
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    sb.append(" NULLS ");
                    sb.append(Randomly.fromOptions("FIRST", "LAST"));
                }
            }
        }

        sb.append(")");
        if (Randomly.getBoolean() && method != IndexType.HASH) {
            sb.append(" INCLUDE(");
            List<YSQLColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
            sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            YSQLExpression expr = new YSQLExpressionGenerator(globalState).setColumns(randomTable.getColumns())
                    .setGlobalState(globalState).generateExpression(YSQLDataType.BOOLEAN);
            sb.append(YSQLVisitor.asString(expr));
        }
        errors.add("already contains data"); // CONCURRENT INDEX failed
        errors.add("You might need to add explicit type casts");
        errors.add("INDEX on column of type");
        errors.add("collations are not supported"); // TODO check
        errors.add("because it has pending trigger events");
        errors.add("duplicate key value violates unique constraint");
        errors.add("could not determine which collation to use for");
        errors.add("index method \"gist\" not supported yet");
        errors.add("is duplicated");
        errors.add("already exists");
        errors.add("could not create unique index");
        errors.add("has no default operator class");
        errors.add("does not support");
        errors.add("cannot cast");
        errors.add("unsupported UNIQUE constraint with partition key definition");
        errors.add("insufficient columns in UNIQUE constraint definition");
        errors.add("invalid input syntax for");
        errors.add("must be type ");
        errors.add("integer out of range");
        errors.add("division by zero");
        errors.add("out of range");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("functions in index expression must be marked IMMUTABLE");
        errors.add("result of range difference would not be contiguous");
        errors.add("which is part of the partition key");
        YSQLErrors.addCommonExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static String getNewIndexName(YSQLTable randomTable) {
        List<YSQLIndex> indexes = randomTable.getIndexes();
        int indexI = 0;
        while (true) {
            String indexName = DBMSCommon.createIndexName(indexI++);
            if (indexes.stream().noneMatch(i -> i.getIndexName().equals(indexName))) {
                return indexName;
            }
        }
    }

    public enum IndexType {
        BTREE, HASH, GIST, GIN
    }

}
