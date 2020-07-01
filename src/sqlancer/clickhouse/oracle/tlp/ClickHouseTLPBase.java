package sqlancer.clickhouse.oracle.tlp;

import ru.yandex.clickhouse.domain.ClickHouseDataType;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseExpression.ClickHouseJoin;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator;
import sqlancer.clickhouse.gen.ClickHouseCommon;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTables;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseTLPBase implements TestOracle {

    final ClickHouseGlobalState state;
    final Set<String> errors = new HashSet<>();

    ClickHouseSchema s;
    ClickHouseTables targetTables;
    ClickHouseExpressionGenerator gen;
    ClickHouseSelect select;
    ClickHouseExpression predicate;
    ClickHouseExpression negatedPredicate;
    ClickHouseExpression isNullPredicate;

    public ClickHouseTLPBase(ClickHouseGlobalState state) {
        this.state = state;
        ClickHouseErrors.addExpectedExpressionErrors(errors);
        ClickHouseErrors.addQueryErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new ClickHouseExpressionGenerator(state).setColumns(targetTables.getColumns());
        select = new ClickHouseSelect();
        select.setFetchColumns(generateFetchColumns());
        List<ClickHouseTable> tables = targetTables.getTables();
        List<ClickHouseJoin> joinStatements = gen.getRandomJoinClauses(tables);
        List<ClickHouseExpression> tableRefs = ClickHouseCommon.getTableRefs(tables, s);
        select.setJoinClauses(joinStatements.stream().collect(Collectors.toList()));
        select.setFromTables(tableRefs);
        select.setWhereClause(null);
        predicate = generatePredicate();
        negatedPredicate = new ClickHouseUnaryPrefixOperation(predicate, ClickHouseUnaryPrefixOperator.NOT);
        isNullPredicate = new ClickHouseUnaryPostfixOperation(predicate, ClickHouseUnaryPostfixOperator.IS_NULL, false);
    }

    List<ClickHouseExpression> generateFetchColumns() {
        List<ClickHouseExpression> columns = new ArrayList<>();
        columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ClickHouseColumnReference(c, null)).collect(Collectors.toList());
        return columns;
    }

    ClickHouseExpression generatePredicate() {
        return gen.generateExpression(new ClickHouseSchema.ClickHouseLancerDataType(ClickHouseDataType.UInt8));
    }

}
