package sqlancer.doris.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisCompositeDataType;
import sqlancer.doris.DorisSchema.DorisDataType;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.DorisSchema.DorisTables;
import sqlancer.doris.ast.DorisCastOperation;
import sqlancer.doris.ast.DorisColumnReference;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisPostfixText;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.ast.DorisTableReference;
import sqlancer.doris.gen.DorisNewExpressionGenerator;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisNoRECOracle extends NoRECBase<DorisGlobalState> implements TestOracle<DorisGlobalState> {

    private final DorisSchema s;

    public DorisNoRECOracle(DorisGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        DorisErrors.addExpressionErrors(errors);
        DorisErrors.addInsertErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        DorisTables randomTables = s.getRandomTableNonEmptyTables();
        List<DorisColumn> columns = randomTables.getColumns();
        DorisNewExpressionGenerator gen = new DorisNewExpressionGenerator(state).setColumns(columns);
        DorisExpression randomWhereCondition = gen.generateExpression(DorisDataType.BOOLEAN);
        List<DorisTable> tables = randomTables.getTables();
        List<DorisTableReference> tableList = tables.stream().map(t -> new DorisTableReference(t))
                .collect(Collectors.toList());
        List<DorisExpression> joins = DorisJoin.getJoins(tableList, state);
        int secondCount = getUnoptimizedQueryCount(tableList.stream().collect(Collectors.toList()),
                randomWhereCondition, joins);
        int firstCount = getOptimizedQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getUnoptimizedQueryCount(List<DorisExpression> tableList, DorisExpression randomWhereCondition,
            List<DorisExpression> joins) throws SQLException {
        DorisSelect select = new DorisSelect();
        DorisExpression asText = new DorisPostfixText(new DorisCastOperation(
                new DorisPostfixText(randomWhereCondition,
                        " IS NOT NULL AND " + DorisToStringVisitor.asString(randomWhereCondition)),
                new DorisCompositeDataType(DorisDataType.INT, 8)), "as count");
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(tableList);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + DorisToStringVisitor.asString(select) + ") as res";
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(SQLConnection con, List<DorisExpression> tableList, List<DorisColumn> columns,
            DorisExpression randomWhereCondition, List<DorisExpression> joins) throws SQLException {
        DorisSelect select = new DorisSelect();
        // select.setGroupByClause(groupBys);
        List<DorisExpression> allColumns = columns.stream().map((c) -> new DorisColumnReference(c))
                .collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            List<DorisExpression> constants = new ArrayList<>();
            constants.add(
                    new DorisConstant.DorisIntConstant(Randomly.smallNumber() % select.getFetchColumns().size() + 1));
            select.setOrderByClauses(constants);
        }
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = DorisToStringVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
