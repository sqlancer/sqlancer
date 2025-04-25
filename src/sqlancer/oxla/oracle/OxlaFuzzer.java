package sqlancer.oxla.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oxla.OxlaGlobalState;
import sqlancer.oxla.ast.OxlaExpression;
import sqlancer.oxla.ast.OxlaSelect;
import sqlancer.oxla.ast.OxlaTableReference;
import sqlancer.oxla.gen.OxlaExpressionGenerator;
import sqlancer.oxla.schema.OxlaDataType;
import sqlancer.oxla.schema.OxlaTables;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OxlaFuzzer implements TestOracle<OxlaGlobalState> {
    private final OxlaGlobalState globalState;
    private final OxlaExpressionGenerator generator;

    public OxlaFuzzer(OxlaGlobalState globalState) {
        this.globalState = globalState;
        this.generator = new OxlaExpressionGenerator(globalState);
    }

    @Override
    public void check() throws Exception {
        try {
            SQLQueryAdapter query = getRandomSelectQuery(Randomly.smallNumber() + 1);
            globalState.executeStatement(query);
            globalState.getManager().incrementSelectQueryCount();
        } catch (Error e) {
            // NOTE
            throw new AssertionError(e);
        }
    }

    // TODO OXLA-8191 Temporary code to be removed when implementing OxlaRandomQueryGenerator.
    private SQLQueryAdapter getRandomSelectQuery(Integer columnCount) {
        OxlaTables nonEmptyTables = new OxlaTables(Randomly.nonEmptySubset(globalState.getSchema().getDatabaseTables()));
        generator.setColumns(nonEmptyTables.getColumns());
        List<OxlaExpression> columns = new ArrayList<>();
        for (int index = 0; index < columnCount; ++index) {
            columns.add(generator.generateExpression(OxlaDataType.getRandomType()));
        }
        OxlaSelect select = new OxlaSelect();
        select.type = OxlaSelect.SelectType.getRandom();
        if (select.type == OxlaSelect.SelectType.DISTINCT && Randomly.getBoolean()) {
            select.distinctOnClause = generator.generateExpression(OxlaDataType.getRandomType());
        }
        // WHAT
        select.setFetchColumns(columns);

        // FROM
        select.setFromList(nonEmptyTables.getTables().stream().map(OxlaTableReference::new).collect(Collectors.toList()));

        // JOIN
        // TODO select.setJoinList();

        // WHERE
        if (Randomly.getBoolean()) {
            select.setWhereClause(generator.generateExpression(OxlaDataType.BOOLEAN));
        }
        // GROUP BY
        if (Randomly.getBoolean()) {
            select.setGroupByClause(generator.generateExpressions(Randomly.smallNumber() + 1));
            // HAVING
            if (Randomly.getBoolean()) {
                select.setHavingClause(generator.generateExpression(OxlaDataType.BOOLEAN));
            }
        }
        // ORDER BY
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(generator.generateExpressions(Randomly.smallNumber() + 1));
        }
        // LIMIT
        if (Randomly.getBoolean()) {
            select.setLimitClause(generator.generateConstant(Randomly.fromOptions(OxlaDataType.NUMERIC)));
        }
        // OFFSET
        if (Randomly.getBoolean()) {
            select.setOffsetClause(generator.generateConstant(Randomly.fromOptions(OxlaDataType.NUMERIC)));
        }
        return new SQLQueryAdapter(select.toString());
    }
}
