package sqlancer.materialize.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeVisitor;
import sqlancer.materialize.ast.MaterializeExpression;

public final class MaterializeInsertGenerator {

    private MaterializeInsertGenerator() {
    }

    public static SQLQueryAdapter insert(MaterializeGlobalState globalState) {
        MaterializeTable table = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("cannot insert into column");
        MaterializeCommon.addCommonExpressionErrors(errors);
        MaterializeCommon.addCommonInsertUpdateErrors(errors);
        MaterializeCommon.addCommonExpressionErrors(errors);
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
        List<MaterializeColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES");

        if (globalState.getDbmsSpecificOptions().allowBulkInsert && Randomly.getBooleanWithSmallProbability()) {
            StringBuilder sbRowValue = new StringBuilder();
            sbRowValue.append("(");
            for (int i = 0; i < columns.size(); i++) {
                if (i != 0) {
                    sbRowValue.append(", ");
                }
                sbRowValue.append(MaterializeVisitor.asString(MaterializeExpressionGenerator
                        .generateConstant(globalState.getRandomly(), columns.get(i).getType())));
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
                insertRow(globalState, sb, columns);
            }
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

    private static void insertRow(MaterializeGlobalState globalState, StringBuilder sb,
            List<MaterializeColumn> columns) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            MaterializeExpression generateConstant;
            if (Randomly.getBoolean()) {
                generateConstant = MaterializeExpressionGenerator.generateConstant(globalState.getRandomly(),
                        columns.get(i).getType());
            } else {
                generateConstant = new MaterializeExpressionGenerator(globalState)
                        .generateExpression(columns.get(i).getType());
            }
            sb.append(MaterializeVisitor.asString(generateConstant));
        }
        sb.append(")");
    }

}
