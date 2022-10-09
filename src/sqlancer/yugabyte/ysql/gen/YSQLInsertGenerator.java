package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;

public final class YSQLInsertGenerator {

    private YSQLInsertGenerator() {
    }

    public static SQLQueryAdapter insert(YSQLGlobalState globalState) {
        YSQLTable table = globalState.getSchema().getRandomTable(YSQLTable::isInsertable);
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("cannot insert into column");
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addCommonInsertUpdateErrors(errors);
        YSQLErrors.addCommonExpressionErrors(errors);
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
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(table.getName());
        List<YSQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" OVERRIDING");
            sb.append(" ");
            sb.append(Randomly.fromOptions("SYSTEM", "USER"));
            sb.append(" VALUE");
        }
        sb.append(" VALUES");

        if (globalState.getDbmsSpecificOptions().allowBulkInsert && Randomly.getBooleanWithSmallProbability()) {
            StringBuilder sbRowValue = new StringBuilder();
            sbRowValue.append("(");
            for (int i = 0; i < columns.size(); i++) {
                if (i != 0) {
                    sbRowValue.append(", ");
                }
                sbRowValue.append(YSQLVisitor.asString(
                        YSQLExpressionGenerator.generateConstant(globalState.getRandomly(), columns.get(i).getType())));
            }
            sbRowValue.append(")");

            int n = (int) Randomly.getNotCachedInteger(100, 1000);
            for (int i = 0; i < n; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(sbRowValue);
            }
        } else {
            int n = Randomly.smallNumber() + 1;
            for (int i = 0; i < n; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                insertRow(globalState, sb, columns, n == 1);
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" ON CONFLICT ");
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(table.getRandomColumn().getName());
                sb.append(")");
                errors.add("there is no unique or exclusion constraint matching the ON CONFLICT specification");
            }
            sb.append(" DO NOTHING");
        }
        errors.add("duplicate key value violates unique constraint");
        errors.add("identity column defined as GENERATED ALWAYS");
        errors.add("out of range");
        errors.add("violates check constraint");
        errors.add("no partition of relation");
        errors.add("invalid input syntax");
        errors.add("division by zero");
        errors.add("violates foreign key constraint");
        errors.add("data type unknown");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static void insertRow(YSQLGlobalState globalState, StringBuilder sb, List<YSQLColumn> columns,
            boolean canBeDefault) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            if (!Randomly.getBooleanWithSmallProbability() || !canBeDefault) {
                YSQLExpression generateConstant;
                if (Randomly.getBoolean()) {
                    generateConstant = YSQLExpressionGenerator.generateConstant(globalState.getRandomly(),
                            columns.get(i).getType());
                } else {
                    generateConstant = new YSQLExpressionGenerator(globalState)
                            .generateExpression(columns.get(i).getType());
                }
                sb.append(YSQLVisitor.asString(generateConstant));
            } else {
                sb.append("DEFAULT");
            }
        }
        sb.append(")");
    }

}
