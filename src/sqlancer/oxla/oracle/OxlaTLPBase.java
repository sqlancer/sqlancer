package sqlancer.oxla.oracle;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.OxlaExpression;
import sqlancer.oxla.ast.OxlaSelect;
import sqlancer.oxla.gen.OxlaExpressionGenerator;
import sqlancer.oxla.schema.OxlaTables;

public class OxlaTLPBase extends TernaryLogicPartitioningOracleBase<OxlaExpression, OxlaGlobalState>
        implements TestOracle<OxlaGlobalState> {
    protected OxlaExpressionGenerator generator;
    protected OxlaTables targetTables;
    protected OxlaSelect select;

    protected OxlaTLPBase(OxlaGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws Exception {
        targetTables = state.getSchema().getRandomTableNonEmptyTables();
        generator = new OxlaExpressionGenerator(state)
                .forceLiteralCasts(true)
                .setTablesAndColumns(targetTables)
                .setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new OxlaSelect();
        select.setFetchColumns(generator.generateFetchColumns(Randomly.getBoolean()));
        select.setJoinClauses(generator.getRandomJoinClauses());
        select.setFromList(generator.getTableRefs());
        select.setWhereClause(null);
    }

    @Override
    protected ExpressionGenerator<OxlaExpression> getGen() {
        return generator;
    }
}
