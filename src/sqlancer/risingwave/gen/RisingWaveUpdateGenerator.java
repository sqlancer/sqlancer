package sqlancer.risingwave.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveColumn;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveToStringVisitor;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveColumnReference;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.ast.RisingWaveFunctionCall;
import sqlancer.risingwave.ast.RisingWaveTableReference;

public final class RisingWaveUpdateGenerator {
    private RisingWaveUpdateGenerator() {
    }

    public static String create(RisingWaveGlobalState state, RisingWaveTable table) {
        if (!table.isWritable()) {
            throw new IllegalArgumentException("UPDATE requires a base table");
        }
        RisingWaveGenerationContext context = new RisingWaveGenerationContext(state, List.of(table));
        RisingWaveExpressionGenerator expressions = new RisingWaveExpressionGenerator(context);
        List<RisingWaveColumnReference> scope = new RisingWaveTableReference(table, table.getName()).columns();
        RisingWaveColumn column = Randomly.fromList(table.getColumns());
        RisingWaveColumnReference reference = scope.get(table.getColumns().indexOf(column));
        RisingWaveExpression value = Randomly.getBoolean() ? reference : expressions.literal(column.getType());
        if (!column.isNullable()) {
            value = new RisingWaveFunctionCall("coalesce", column.getType(), List.of(value, reference));
        }
        return "UPDATE " + RisingWaveToStringVisitor.identifier(table.getName()) + " SET "
                + RisingWaveToStringVisitor.identifier(column.getName()) + " = "
                + RisingWaveToStringVisitor.asString(value)
                + (Randomly.getBooleanWithSmallProbability() ? "" : " WHERE " + RisingWaveToStringVisitor
                        .asString(expressions.expression(RisingWaveType.BOOLEAN, scope, 0, false)));
    }
}
