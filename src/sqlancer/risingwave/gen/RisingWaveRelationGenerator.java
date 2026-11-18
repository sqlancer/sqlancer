package sqlancer.risingwave.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveColumnReference;
import sqlancer.risingwave.ast.RisingWaveCteReference;
import sqlancer.risingwave.ast.RisingWaveDerivedTable;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.ast.RisingWaveJoin;
import sqlancer.risingwave.ast.RisingWaveOperation.Operator;
import sqlancer.risingwave.ast.RisingWaveRelation;
import sqlancer.risingwave.ast.RisingWaveSelect;
import sqlancer.risingwave.ast.RisingWaveTableReference;
import sqlancer.risingwave.ast.RisingWaveWindow;
import sqlancer.risingwave.ast.RisingWaveWith;

final class RisingWaveRelationGenerator {
    private enum RelationKind {
        TABLE, JOIN, DERIVED_TABLE, CTE, WINDOW
    }

    private final RisingWaveGenerationContext context;
    private final RisingWaveExpressionGenerator expressions;

    RisingWaveRelationGenerator(RisingWaveGenerationContext context, RisingWaveExpressionGenerator expressions) {
        this.context = context;
        this.expressions = expressions;
    }

    RisingWaveRelation generate(int depth) {
        if (depth <= 0 || --context.relationBudget <= 0) {
            return context.table();
        }
        switch (Randomly.fromOptions(RelationKind.TABLE, RelationKind.JOIN, RelationKind.JOIN,
                RelationKind.DERIVED_TABLE, RelationKind.DERIVED_TABLE, RelationKind.CTE, RelationKind.WINDOW)) {
        case TABLE:
            return context.table();
        case JOIN:
            return join(generate(depth - 1), generate(depth - 1));
        case DERIVED_TABLE:
            RisingWaveRelation source = generate(depth - 1);
            RisingWaveSelect query = RisingWaveQueryGenerator.block(context, expressions, source,
                    Randomly.getBoolean());
            return new RisingWaveDerivedTable(query, context.alias());
        case CTE:
            return with(generate(depth - 1));
        case WINDOW:
            return window();
        default:
            throw new AssertionError();
        }
    }

    private RisingWaveRelation join(RisingWaveRelation left, RisingWaveRelation right) {
        List<RisingWaveColumnReference> eligible = left.columns().stream()
                .filter(a -> !a.type.array() && !a.type.struct())
                .filter(a -> right.columns().stream().anyMatch(b -> a.type == b.type)).collect(Collectors.toList());
        if (eligible.isEmpty()) {
            return left;
        }
        List<RisingWaveColumnReference> keys = Randomly.nonEmptySubset(eligible);
        RisingWaveExpression condition = null;
        for (int i = 0; i < Math.min(3, keys.size()); i++) {
            RisingWaveColumnReference a = keys.get(i);
            List<RisingWaveColumnReference> matches = right.columns().stream().filter(b -> b.type == a.type)
                    .collect(Collectors.toList());
            RisingWaveExpression comparison = expressions.joinComparison(a, matches);
            condition = condition == null ? comparison
                    : expressions.binary(Operator.AND, RisingWaveType.BOOLEAN, condition, comparison);
        }
        return new RisingWaveJoin(left, right, Randomly.fromOptions(RisingWaveJoin.Kind.values()), condition);
    }

    private RisingWaveRelation window() {
        RisingWaveTableReference table = context.table();
        if (context.streaming) {
            return table;
        }
        List<RisingWaveColumnReference> timestamps = table.columns().stream()
                .filter(c -> c.type == RisingWaveType.TIMESTAMP).collect(Collectors.toList());
        if (!context.options.timeWindows || timestamps.isEmpty()) {
            return table;
        }
        boolean hop = Randomly.getBoolean();
        int slide = Randomly.fromOptions(1, 60, 300, 3600, 86400);
        int expansion = hop ? (int) Randomly.getNotCachedInteger(1, context.options.maxWindowExpansion + 1) : 1;
        return new RisingWaveWindow(table, Randomly.fromList(timestamps).name, context.alias(), hop, slide,
                slide * expansion);
    }

    private RisingWaveRelation with(RisingWaveRelation source) {
        List<RisingWaveWith.Definition> definitions = new ArrayList<>();
        RisingWaveSelect definition = RisingWaveQueryGenerator.block(context, expressions, source,
                Randomly.getBoolean());
        String name = context.alias();
        definitions.add(new RisingWaveWith.Definition(name, definition));
        RisingWaveRelation reference = new RisingWaveCteReference(name, context.alias(), definition);
        if (Randomly.getBoolean() && --context.relationBudget > 0) {
            // The second definition depends only on the already-bound first definition.
            RisingWaveSelect second = RisingWaveQueryGenerator.block(context, expressions, reference,
                    Randomly.getBoolean());
            name = context.alias();
            definitions.add(new RisingWaveWith.Definition(name, second));
            definition = second;
            reference = new RisingWaveCteReference(name, context.alias(), second);
        }
        if (Randomly.getBoolean() && --context.relationBudget > 0) {
            RisingWaveRelation other = new RisingWaveCteReference(name, context.alias(), definition);
            reference = join(reference, other);
        }
        RisingWaveSelect consumer = RisingWaveQueryGenerator.block(context, expressions, reference,
                Randomly.getBoolean());
        return new RisingWaveWith(definitions, consumer, context.alias());
    }
}
