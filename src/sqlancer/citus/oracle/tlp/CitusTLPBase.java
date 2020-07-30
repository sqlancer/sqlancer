package sqlancer.citus.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.citus.CitusGlobalState;
import sqlancer.citus.CitusSchema.CitusTable;
import sqlancer.citus.gen.CitusCommon;
import sqlancer.postgres.PostgresGlobalState;
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
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class CitusTLPBase extends PostgresTLPBase {

    HashMap<PostgresTable, Integer> distributedTables;
    List<PostgresTable> referenceTables;
    List<PostgresTable> localTables;

    public CitusTLPBase(CitusGlobalState state) {
        super(state);
        CitusCommon.addCitusErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        // clear left-over query string from previous test
        state.getState().queryString = null;
        s = state.getSchema();
        storeCitusTableTypes();
        List<PostgresTable> tables = null;
        List<PostgresJoin> joins = generateJoins(tables);
        generateSelectBase(tables, joins);
    }

    private List<PostgresJoin> generateJoins(List<PostgresTable> tables) {
        List<PostgresJoin> joins = null;
        if (distributedTables.isEmpty()
                || (!referenceTables.isEmpty() && !Randomly.getBooleanWithRatherLowProbability())) {
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
            tables = new ArrayList<>(targetTables.getTables());
            joins = getJoinStatements(state, targetTables.getColumns(), tables);
        } else {
            // joins between distributed tables
            // join including distribution columns
            // supports complex joins if colocated
            tables = Randomly.nonEmptySubset(new ArrayList<>(distributedTables.keySet()));
            targetTables = new PostgresTables(tables);
            CitusTable fromTable = (CitusTable) Randomly.fromList(tables);
            joins = getCitusJoinStatements(state, tables, fromTable);
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

    List<PostgresJoin> getCitusJoinStatements(PostgresGlobalState globalState, List<PostgresTable> joinTables,
            CitusTable fromTable) {
        List<PostgresColumn> columns = new ArrayList<>();
        for (PostgresTable t : joinTables) {
            columns.add(((CitusTable) t).getDistributionColumn());
        }
        List<PostgresJoin> joinStatements = new ArrayList<>();
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(columns);
        joinTables.remove(fromTable);
        boolean allColocated = true;
        for (PostgresTable t : joinTables) {
            boolean colocated = (distributedTables.get(fromTable) == distributedTables.get(t));
            allColocated = allColocated && colocated;
        }
        while (!joinTables.isEmpty()) {
            CitusTable table = (CitusTable) Randomly.fromList(joinTables);
            // FIXME: can I remove even if reference types don't match due to casting?
            joinTables.remove(table);
            PostgresExpression joinClause = null;
            PostgresExpression equiJoinClause = null;
            if (allColocated) {
                PostgresExpression leftExpr = new PostgresColumnValue(fromTable.getDistributionColumn(), null);
                PostgresExpression rightExpr = new PostgresColumnValue(table.getDistributionColumn(), null);
                equiJoinClause = new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
                        PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.EQUALS);
            } else {
                // check if repartition joins are allowed
                if (!((CitusGlobalState) globalState).getRepartition()) {
                    continue;
                }
                PostgresExpression leftExpr = new PostgresColumnValue(fromTable.getDistributionColumn(), null);
                List<PostgresColumn> candidateRightColumns = table.getColumns().stream()
                        .filter(c -> c.getType().equals(fromTable.getDistributionColumn().getType()))
                        .collect(Collectors.toList());
                if (candidateRightColumns.isEmpty()) {
                    continue;
                }
                PostgresExpression rightExpr = new PostgresColumnValue(Randomly.fromList(candidateRightColumns), null);
                equiJoinClause = new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
                        PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.EQUALS);
            }
            if (allColocated && Randomly.getBooleanWithSmallProbability()) {
                joinClause = new PostgresBinaryLogicalOperation(equiJoinClause,
                        gen.generateExpression(PostgresDataType.BOOLEAN),
                        PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
            } else {
                joinClause = equiJoinClause;
            }
            PostgresJoinType options = Randomly.fromOptions(PostgresJoinType.INNER, PostgresJoinType.LEFT,
                    PostgresJoinType.RIGHT, PostgresJoinType.FULL);
            if (!allColocated) {
                options = PostgresJoinType.INNER;
            }
            PostgresJoin j = new PostgresJoin(new PostgresFromTable(table, Randomly.getBoolean()), joinClause, options);
            joinStatements.add(j);
        }
        joinTables.add(fromTable);
        return joinStatements;
    }

    private void addSubqueryJoinStatements(PostgresGlobalState globalState, List<PostgresJoin> joinStatements,
            PostgresTable fromTable) {
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            PostgresTables subqueryTables = new PostgresTables(Randomly.nonEmptySubset(localTables));
            List<PostgresColumn> columns = subqueryTables.getColumns();
            columns.addAll(fromTable.getColumns());
            PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(columns);
            PostgresExpression subquery = createSubquery(globalState, String.format("sub%d", i), subqueryTables);
            PostgresExpression joinClause = gen.generateExpression(PostgresDataType.BOOLEAN);
            PostgresJoinType options = PostgresJoinType.getRandom();
            PostgresJoin j = new PostgresJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }

    }

}
