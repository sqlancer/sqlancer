package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.citus.CitusGlobalState;
import sqlancer.citus.CitusSchema.CitusTable;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresJoin.PostgresJoinType;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class CitusTLPBase extends PostgresTLPBase {

    Map<PostgresTable, Integer> distributedTables;
    List<PostgresTable> referenceTables;
    List<PostgresTable> localTables;

    public CitusTLPBase(CitusGlobalState state) {
        super(state);
        CitusCommon.addCitusErrors(errors);
    }

    public PostgresSchema getSchema() {
        return s;
    }

    public PostgresTables getTargetTables() {
        return targetTables;
    }

    public PostgresExpressionGenerator getGenerator() {
        return gen;
    }

    public PostgresSelect getSelect() {
        return select;
    }

    public PostgresExpression getPredicate() {
        return predicate;
    }

    public PostgresExpression getNegatedPredicate() {
        return negatedPredicate;
    }

    public PostgresExpression getIsNullPredicate() {
        return isNullPredicate;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        storeCitusTableTypes();
        List<PostgresTable> tables = new ArrayList<>();
        List<PostgresJoin> joins = generateJoins(tables);
        generateSelectBase(tables, joins);
    }

    private List<PostgresJoin> generateJoins(List<PostgresTable> tables) {
        List<PostgresJoin> joins = null;
        tables.clear();
        if (distributedTables.isEmpty()
                || !referenceTables.isEmpty() && !Randomly.getBooleanWithRatherLowProbability()) {
            if (!localTables.isEmpty()) {
                // joins including only local tables
                // supports complex joins
                targetTables = new PostgresTables(Randomly.nonEmptySubset(localTables));
            }
            if (!referenceTables.isEmpty()) {
                // joins including reference tables
                // supports complex joins
                List<PostgresTable> targetTableList = new ArrayList<>(referenceTables);
                if (!distributedTables.isEmpty()) {
                    // joins including distributed and reference tables
                    // supports complex joins
                    targetTableList.add(Randomly.fromList(new ArrayList<>(distributedTables.keySet())));
                }
                targetTables = new PostgresTables(Randomly.nonEmptySubset(targetTableList));
            }
            tables.addAll(targetTables.getTables());
            joins = getJoinStatements(state, targetTables.getColumns(), tables);
        } else {
            // joins between distributed tables
            // join including distribution columns
            // supports complex joins if colocated
            tables.addAll(Randomly.nonEmptySubset(new ArrayList<>(distributedTables.keySet())));
            targetTables = new PostgresTables(tables);
            CitusTable fromTable = (CitusTable) Randomly.fromList(tables);
            joins = getCitusJoinStatements((CitusGlobalState) state, tables, fromTable);
            if (Randomly.getBooleanWithRatherLowProbability() && !localTables.isEmpty()) {
                addSubqueryJoinStatements(state, joins, fromTable);
            }
        }
        return joins;
    }

    private void storeCitusTableTypes() {
        distributedTables = new HashMap<>();
        referenceTables = new ArrayList<>();
        localTables = new ArrayList<>();
        for (PostgresTable table : s.getDatabaseTables()) {
            CitusTable citusTable = (CitusTable) table;
            Integer colocationId = citusTable.getColocationId();
            PostgresColumn distributionColumn = citusTable.getDistributionColumn();
            if (colocationId != null && distributionColumn != null) {
                distributedTables.put(citusTable, colocationId);
            } else if (colocationId != null) {
                referenceTables.add(citusTable);
            } else {
                localTables.add(citusTable);
            }
        }
    }

    private PostgresJoin allColocatedJoins(CitusTable joinTable, CitusTable fromTable,
            PostgresExpressionGenerator citusJoinGen) {
        PostgresExpression leftExpr = new PostgresColumnValue(fromTable.getDistributionColumn(), null);
        PostgresExpression rightExpr = new PostgresColumnValue(joinTable.getDistributionColumn(), null);
        // JOIN over equality between the distribution columns of the tables being joined
        PostgresExpression equiJoinClause = new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
                PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.EQUALS);
        PostgresExpression joinClause = null;
        if (Randomly.getBooleanWithSmallProbability()) {
            // add randomly generated boolean statement to JOIN clause
            joinClause = new PostgresBinaryLogicalOperation(equiJoinClause,
                    citusJoinGen.generateExpression(PostgresDataType.BOOLEAN),
                    PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
        } else {
            joinClause = equiJoinClause;
        }
        PostgresJoinType options = Randomly.fromOptions(PostgresJoinType.INNER, PostgresJoinType.LEFT,
                PostgresJoinType.RIGHT, PostgresJoinType.FULL);
        return new PostgresJoin(new PostgresFromTable(joinTable, Randomly.getBoolean()), joinClause, options);
    }

    private PostgresJoin repartitionJoins(CitusTable joinTable, CitusTable fromTable) {
        PostgresExpression leftExpr = new PostgresColumnValue(fromTable.getDistributionColumn(), null);
        List<PostgresColumn> candidateRightColumns = joinTable.getColumns().stream()
                .filter(c -> c.getType().equals(fromTable.getDistributionColumn().getType()))
                .collect(Collectors.toList());
        if (candidateRightColumns.isEmpty()) {
            return null;
        }
        PostgresExpression rightExpr = new PostgresColumnValue(Randomly.fromList(candidateRightColumns), null);
        // JOIN over equality between the distribution column of one table and a column that matches the data type from
        // the other table being joined
        PostgresExpression joinClause = new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
                PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.EQUALS);
        PostgresJoinType options = PostgresJoinType.INNER;
        return new PostgresJoin(new PostgresFromTable(joinTable, Randomly.getBoolean()), joinClause, options);
    }

    List<PostgresJoin> getCitusJoinStatements(CitusGlobalState globalState, List<PostgresTable> joinTables,
            CitusTable fromTable) {
        List<PostgresColumn> columns = new ArrayList<>();
        for (PostgresTable t : joinTables) {
            columns.add(((CitusTable) t).getDistributionColumn());
        }
        List<PostgresJoin> joinStatements = new ArrayList<>();
        PostgresExpressionGenerator citusJoinGen = new PostgresExpressionGenerator(globalState).setColumns(columns);
        joinTables.remove(fromTable);
        // check if all tables being joined are colocated
        boolean allColocated = true;
        for (PostgresTable t : joinTables) {
            boolean colocated = distributedTables.get(fromTable).equals(distributedTables.get(t));
            allColocated = allColocated && colocated;
        }
        while (!joinTables.isEmpty()) {
            CitusTable table = (CitusTable) Randomly.fromList(joinTables);
            joinTables.remove(table);
            PostgresJoin j = null;
            if (allColocated) {
                j = allColocatedJoins(table, fromTable, citusJoinGen);
                // check if repartition joins are allowed if all tables are not colocated
            } else if (globalState.getRepartition()) {
                j = repartitionJoins(table, fromTable);
            }
            if (j != null) {
                joinStatements.add(j);
            }
        }
        joinTables.add(fromTable);
        return joinStatements;
    }

    private void addSubqueryJoinStatements(PostgresGlobalState globalState, List<PostgresJoin> joinStatements,
            PostgresTable fromTable) {
        // JOIN with subquery
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            PostgresTables subqueryTables = new PostgresTables(Randomly.nonEmptySubset(localTables));
            List<PostgresColumn> columns = new ArrayList<>();
            columns.addAll(subqueryTables.getColumns());
            columns.addAll(fromTable.getColumns());
            PostgresExpression subquery = createSubquery(globalState, String.format("sub%d", i), subqueryTables);
            PostgresExpressionGenerator subqueryJoinGen = new PostgresExpressionGenerator(globalState)
                    .setColumns(columns);
            PostgresExpression joinClause = subqueryJoinGen.generateExpression(PostgresDataType.BOOLEAN);
            PostgresJoinType options = PostgresJoinType.getRandom();
            PostgresJoin j = new PostgresJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }

    }

}
