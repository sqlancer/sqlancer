package sqlancer.risingwave.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveToStringVisitor;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveTableReference;

public final class RisingWaveDeleteGenerator {
    private RisingWaveDeleteGenerator() {
    }

    public static String create(RisingWaveGlobalState state, RisingWaveTable table) {
        if (!table.isWritable()) {
            throw new IllegalArgumentException("DELETE requires a base table");
        }
        RisingWaveGenerationContext context = new RisingWaveGenerationContext(state, List.of(table));
        RisingWaveExpressionGenerator expressions = new RisingWaveExpressionGenerator(context);
        return "DELETE FROM " + RisingWaveToStringVisitor.identifier(table.getName())
                + (Randomly.getBooleanWithSmallProbability() ? ""
                        : " WHERE " + RisingWaveToStringVisitor.asString(expressions.expression(RisingWaveType.BOOLEAN,
                                new RisingWaveTableReference(table, table.getName()).columns(), 0, false)));
    }
}
