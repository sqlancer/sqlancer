package sqlancer.sqlite3.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3Provider.SQLite3GlobalState;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3TLPBase implements TestOracle {

    final SQLite3GlobalState state;
    final Set<String> errors = new HashSet<>();

    SQLite3Schema s;
    SQLite3Tables targetTables;
    SQLite3ExpressionGenerator gen;
    SQLite3Select select;
    SQLite3Expression predicate;
    SQLite3Expression negatedPredicate;
    SQLite3Expression isNullPredicate;

    public SQLite3TLPBase(SQLite3GlobalState state) {
        this.state = state;
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new SQLite3ExpressionGenerator(state).setColumns(targetTables.getColumns());
        select = new SQLite3Select();
        select.setFetchColumns(generateFetchColumns());
        List<SQLite3Table> tables = targetTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
        select.setJoinClauses(joinStatements.stream().collect(Collectors.toList()));
        select.setFromTables(tableRefs);
        select.setWhereClause(null);
        predicate = generatePredicate();
        negatedPredicate = new SQLite3UnaryOperation(UnaryOperator.NOT, predicate);
        isNullPredicate = new SQLite3PostfixUnaryOperation(PostfixUnaryOperator.ISNULL, predicate);
    }

    List<SQLite3Expression> generateFetchColumns() {
        List<SQLite3Expression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new SQLite3ColumnName(SQLite3Column.createDummy("*"), null));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new SQLite3ColumnName(c, null)).collect(Collectors.toList());
        }
        return columns;
    }

    SQLite3Expression generatePredicate() {
        return gen.generateExpression();
    }

}
