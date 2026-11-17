package sqlancer.risingwave.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.RisingWaveOptions;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.ast.RisingWaveTableReference;

final class RisingWaveGenerationContext {
    final RisingWaveOptions options;
    final int expressionDepth;
    final List<RisingWaveTable> tables;
    int expressionBudget;
    // JOINs, subqueries and CTEs share this budget to limit query size.
    int relationBudget;
    int scalarSubqueryBudget = 1;
    int correlatedExistsBudget = 1;
    boolean streaming;
    private int nextAlias;

    RisingWaveGenerationContext(RisingWaveGlobalState state, List<RisingWaveTable> tables) {
        this.options = state.getDbmsSpecificOptions();
        options.validate();
        expressionDepth = state.getOptions().getMaxExpressionDepth();
        this.tables = List.copyOf(tables);
        expressionBudget = options.expressionNodes;
        relationBudget = options.relationNodes;
    }

    String alias() {
        return "r" + nextAlias++;
    }

    RisingWaveTableReference table() {
        return new RisingWaveTableReference(Randomly.fromList(tables), alias());
    }
}
