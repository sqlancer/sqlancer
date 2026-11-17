package sqlancer.risingwave.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveToStringVisitor;
import sqlancer.risingwave.ast.RisingWaveRelation;
import sqlancer.risingwave.ast.RisingWaveSelect;

public final class RisingWaveViewGenerator {
    private RisingWaveViewGenerator() {
    }

    public static String create(RisingWaveGlobalState state, List<RisingWaveTable> bases, String name,
            boolean materialized) {
        RisingWaveSelect query = definition(state, bases, materialized);
        List<String> names = new ArrayList<>();
        for (int i = 0; i < query.getFetchColumns().size(); i++) {
            names.add("c" + i);
        }
        return "CREATE " + (materialized ? "MATERIALIZED " : "") + "VIEW " + RisingWaveToStringVisitor.identifier(name)
                + " (" + String.join(", ", names) + ") AS " + query.asString();
    }

    public static RisingWaveSelect definition(RisingWaveGlobalState state, List<RisingWaveTable> bases,
            boolean materialized) {
        RisingWaveGenerationContext context = new RisingWaveGenerationContext(state, bases);
        context.streaming = materialized;
        RisingWaveExpressionGenerator expressions = new RisingWaveExpressionGenerator(context);
        // Use the verified streaming subset: equality joins, no scalar subqueries or time windows.
        RisingWaveRelation source = Randomly.getBoolean() ? context.table()
                : new RisingWaveRelationGenerator(context, expressions)
                        .generate(Math.min(materialized ? 2 : 3, context.options.relationDepth));
        return RisingWaveQueryGenerator.block(context, expressions, source, Randomly.getBoolean());
    }
}
