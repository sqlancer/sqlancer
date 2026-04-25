package sqlancer.materialize.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.materialize.MaterializeGlobalState;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;
import sqlancer.materialize.MaterializeVisitor;

public final class MaterializeDeleteGenerator extends AbstractDeleteGenerator {

    private final MaterializeGlobalState globalState;

    private MaterializeDeleteGenerator(MaterializeGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(MaterializeGlobalState globalState) {
        return new MaterializeDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        MaterializeTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        errors.add("violates foreign key constraint");
        errors.add("violates not-null constraint");
        errors.add("could not determine which collation to use for string comparison");
        appendDeleteFromTable(table.getName());
        if (Randomly.getBoolean()) {
            appendWhereClause(MaterializeVisitor.asString(MaterializeExpressionGenerator.generateExpression(globalState,
                    table.getColumns(), MaterializeDataType.BOOLEAN)));
        }
        MaterializeCommon.addCommonExpressionErrors(errors);
        errors.add("out of range");
        errors.add("does not support casting");
        errors.add("invalid input syntax for");
        errors.add("division by zero");
    }

}
