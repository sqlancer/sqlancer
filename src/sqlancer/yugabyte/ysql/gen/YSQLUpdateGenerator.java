package sqlancer.yugabyte.ysql.gen;

import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;
import sqlancer.yugabyte.ysql.YSQLVisitor;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;

public final class YSQLUpdateGenerator extends AbstractUpdateGenerator<YSQLColumn> {

    private final YSQLGlobalState globalState;
    private YSQLTable randomTable;

    private YSQLUpdateGenerator(YSQLGlobalState globalState) {
        this.globalState = globalState;
        errors.addAll(Arrays.asList("conflicting key value violates exclusion constraint",
                "reached maximum value of sequence", "violates foreign key constraint", "violates not-null constraint",
                "violates unique constraint", "out of range", "cannot cast", "must be type boolean", "is not unique",
                " bit string too long", "can only be updated to DEFAULT", "division by zero",
                "You might need to add explicit type casts.", "invalid regular expression",
                "View columns that are not columns of their base relation are not updatable"));
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        return new YSQLUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        randomTable = globalState.getSchema().getRandomTable(YSQLTable::isInsertable);
        List<YSQLColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
        sb.append("UPDATE ");
        sb.append(randomTable.getName());
        sb.append(" SET ");
        errors.add("multiple assignments to same column"); // view whose columns refer to a column in the referenced
        // table multiple times
        errors.add("new row violates check option for view");
        YSQLErrors.addCommonInsertUpdateErrors(errors);

        updateColumns(columns);
        errors.add("invalid input syntax for ");
        errors.add("operator does not exist: text = boolean");
        errors.add("violates check constraint");
        errors.add("could not determine which collation to use for string comparison");
        errors.add("but expression is of type");
        YSQLErrors.addCommonExpressionErrors(errors);
        YSQLErrors.addTransactionErrors(errors);
        if (!Randomly.getBooleanWithSmallProbability()) {
            sb.append(" WHERE ");
            YSQLExpression where = YSQLExpressionGenerator.generateExpression(globalState, randomTable.getColumns(),
                    YSQLDataType.BOOLEAN);
            sb.append(YSQLVisitor.asString(where));
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    @Override
    protected void updateValue(YSQLColumn column) {
        YSQLDataType type = column.getType();
        // For special types that can't be cast from arbitrary expressions, use constants
        boolean isSpecialType = type == YSQLDataType.RANGE || type == YSQLDataType.INT4RANGE || 
                               type == YSQLDataType.INT8RANGE || type == YSQLDataType.NUMRANGE ||
                               type == YSQLDataType.TSRANGE || type == YSQLDataType.TSTZRANGE || 
                               type == YSQLDataType.DATERANGE || type == YSQLDataType.CIDR ||
                               type == YSQLDataType.INET || type == YSQLDataType.MACADDR ||
                               type == YSQLDataType.UUID || type == YSQLDataType.POINT ||
                               type == YSQLDataType.LINE || type == YSQLDataType.LSEG ||
                               type == YSQLDataType.BOX || type == YSQLDataType.PATH ||
                               type == YSQLDataType.POLYGON || type == YSQLDataType.CIRCLE;
        
        if (!Randomly.getBoolean() || isSpecialType) {
            YSQLExpression constant = YSQLExpressionGenerator.generateConstant(globalState.getRandomly(),
                    column.getType());
            sb.append(YSQLVisitor.asString(constant));
        } else if (Randomly.getBoolean()) {
            sb.append("DEFAULT");
        } else {
            sb.append("(");
            YSQLExpression expr = YSQLExpressionGenerator.generateExpression(globalState, randomTable.getColumns(),
                    column.getType());
            sb.append(YSQLVisitor.asString(expr));
            sb.append(")");
        }
    }

}
