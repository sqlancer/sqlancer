package sqlancer.mysql.test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.TestOracle;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public abstract class MySQLQueryPartitioningBase implements TestOracle {

    final MySQLGlobalState state;
    final Set<String> errors = new HashSet<>();

    MySQLSchema s;
    MySQLTables targetTables;
    MySQLExpressionGenerator gen;
    MySQLSelect select;
    MySQLExpression predicate;
    MySQLExpression negatedPredicate;
    MySQLExpression isNullPredicate;

    public MySQLQueryPartitioningBase(MySQLGlobalState state) {
        this.state = state;
        MySQLErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new MySQLExpressionGenerator(state).setColumns(targetTables.getColumns());
        select = new MySQLSelect();
        select.setFetchColumns(generateFetchColumns());
        List<MySQLTable> tables = targetTables.getTables();
        List<MySQLExpression> tableList = tables.stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        // List<MySQLExpression> joins = MySQLJoin.getJoins(tableList, state);
        select.setFromList(tableList);
        select.setWhereClause(null);
        // select.setJoins(joins);
        predicate = generatePredicate();
        negatedPredicate = new MySQLUnaryPrefixOperation(predicate, MySQLUnaryPrefixOperator.NOT);
        isNullPredicate = new MySQLUnaryPostfixOperation(predicate,
                MySQLUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    List<MySQLExpression> generateFetchColumns() {
        return Arrays.asList(MySQLColumnReference.create(targetTables.getColumns().get(0), null));
    }

    MySQLExpression generatePredicate() {
        return gen.generateExpression();
    }

}
