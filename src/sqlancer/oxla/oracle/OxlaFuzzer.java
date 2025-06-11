package sqlancer.oxla.oracle;

import sqlancer.IgnoreMeException;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.gen.*;
import sqlancer.oxla.util.RandomCollection;

public class OxlaFuzzer implements TestOracle<OxlaGlobalState> {
    private final OxlaGlobalState globalState;
    private final RandomCollection<OxlaQueryGenerator> generators;

    public OxlaFuzzer(OxlaGlobalState globalState) {
        this.globalState = globalState;

        final var options = globalState.getDbmsSpecificOptions();
        generators = new RandomCollection<OxlaQueryGenerator>()
                .add(1, new OxlaAlterGenerator())
                .add(1, new OxlaCreateIndexGenerator())
                .add(1, new OxlaCreateRoleGenerator())
                .add(1, new OxlaCreateSchemaGenerator())
                .add(15, new OxlaCreateTableGenerator())
                .add(1, new OxlaCreateTypeGenerator())
                .add(1, new OxlaDeallocateGenerator())
                .add(1, new OxlaDeleteFromGenerator())
                .add(1, new OxlaDropRoleGenerator())
                .add(1, new OxlaDropSchemaGenerator())
                .add(5, new OxlaDropTableGenerator())
                .add(1, new OxlaDropTypeGenerator())
                .add(1, new OxlaDiscardGenerator())
                .add(50, new OxlaInsertIntoGenerator())
                .add(1, new OxlaPrivilegeGenerator())
                .add(200, new OxlaSelectGenerator())
                .add(1, new OxlaSetGenerator())
                .add(1, new OxlaShowGenerator())
                .add(1, new OxlaTransactionGenerator(), options.enableTransactionTesting)
                .add(1, new OxlaUpdateGenerator())
        ;
    }

    @Override
    public void check() throws Exception {
        final OxlaQueryGenerator generator = generators.getRandom();
        final SQLQueryAdapter query = generator.getQuery(globalState, 0);
        try {
            globalState.executeStatement(query);
            globalState.getManager().incrementSelectQueryCount();
            if (generator.modifiesDatabaseState()) {
                globalState.updateSchema();
            }
        } catch (Error e) {
            if (query.getExpectedErrors().errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(e);
        }
    }
}
