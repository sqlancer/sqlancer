package sqlancer.sqlite3.oracle;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.CODDTestBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Aggregate;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Constant.SQLite3TextConstant;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.InOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Alias;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Exist;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ExpressionBag;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3TableAndColumnRef;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3TableReference;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ResultMap;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Typeof;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Values;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3WithClasure;
import sqlancer.sqlite3.ast.SQLite3Expression.Join.JoinType;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm.Ordering;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation.BinaryOperator;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3CODDTestOracle extends CODDTestBase<SQLite3GlobalState> implements TestOracle<SQLite3GlobalState> {

    private final SQLite3Schema s;
    private SQLite3ExpressionGenerator gen;
    private Reproducer<SQLite3GlobalState> reproducer;

    private String tempTableName = "temp_table";

    private SQLite3Expression foldedExpr;
    private SQLite3Expression constantResOfFoldedExpr;

    private List<SQLite3Table> tablesFromOuterContext = new ArrayList<>();
    private List<Join> joinsInExpr = null;

    Map<String, List<SQLite3Constant>> auxiliaryQueryResult = new HashMap<>();
    Map<String, List<SQLite3Constant>> selectResult = new HashMap<>();

    Boolean useSubqueryAsFoldedExpr;
    Boolean useCorrelatedSubqueryAsFoldedExpr;

    public SQLite3CODDTestOracle(SQLite3GlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addMatchQueryErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        errors.add("misuse of aggregate");
        errors.add("misuse of window function");
        errors.add("second argument to nth_value must be a positive integer");
        errors.add("no such table");
        errors.add("no query solution");
        errors.add("unable to use function MATCH in the requested context");
        errors.add("[SQLITE_ERROR] SQL error or missing database (unrecognized token:");
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;

        useSubqueryAsFoldedExpr = useSubquery();
        useCorrelatedSubqueryAsFoldedExpr = useCorrelatedSubquery();

        SQLite3Select auxiliaryQuery = null;
        if (useSubqueryAsFoldedExpr) {
            if (useCorrelatedSubqueryAsFoldedExpr) {
                auxiliaryQuery = genSelectWithCorrelatedSubquery(null, null);
                auxiliaryQueryString = SQLite3Visitor.asString(auxiliaryQuery);

                auxiliaryQueryResult.putAll(selectResult);
            } else {
                auxiliaryQuery = genSelectExpression(null, null);
                auxiliaryQueryString = SQLite3Visitor.asString(auxiliaryQuery);
                auxiliaryQueryResult = getQueryResult(auxiliaryQueryString, state);
            }
        } else {
            auxiliaryQuery = genSimpleSelect(null, null);
            auxiliaryQueryString = SQLite3Visitor.asString(auxiliaryQuery);

            auxiliaryQueryResult.putAll(selectResult);
        }
        

        SQLite3Select originalQuery = null;
        
        Map<String, List<SQLite3Constant>> foldedResult = new HashMap<>();
        Map<String, List<SQLite3Constant>> originalResult = new HashMap<>();

        // dependent expression
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            // original query
            SQLite3ExpressionBag specificCondition = new SQLite3ExpressionBag(this.foldedExpr);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = SQLite3Visitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            specificCondition.updateInnerExpr(this.constantResOfFoldedExpr);
            foldedQueryString = SQLite3Visitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // independent expression
        // empty result, put the inner query in (NOT) EXIST
        else if (auxiliaryQueryResult.size() == 0 || auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().iterator().next()).size() == 0) {
            boolean isNegated = Randomly.getBoolean() ? false : true;
            // original query
            SQLite3Exist existExpr = new SQLite3Exist(new SQLite3Select(auxiliaryQuery), isNegated);
            SQLite3ExpressionBag specificCondition = new SQLite3ExpressionBag(existExpr);
            
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = SQLite3Visitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            
            // folded query
            SQLite3Expression equivalentExpr = isNegated ? SQLite3Constant.createTrue() : SQLite3Constant.createFalse();
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = SQLite3Visitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // Scalar Subquery: 1 column and 1 row, consider the inner query as a constant
        else if (auxiliaryQueryResult.size() == 1 && auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size() == 1 && Randomly.getBoolean()) {
            // original query
            SQLite3ExpressionBag specificCondition = new SQLite3ExpressionBag(auxiliaryQuery);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = SQLite3Visitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            
            // folded query
            SQLite3Expression equivalentExpr = auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).get(0);
            specificCondition.updateInnerExpr(equivalentExpr);;
            foldedQueryString = SQLite3Visitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // one column
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBooleanWithRatherLowProbability()) {
        // else if (auxiliaryQueryResult.size() == 1 && false) {
            // original query
            List<SQLite3Column> columns = s.getRandomTableNonEmptyTables().getColumns();
            SQLite3ColumnName selectedColumn = new SQLite3ColumnName(Randomly.fromList(columns), null);
            SQLite3Table selectedTable = selectedColumn.getColumn().getTable();
            InOperation INOperation = new InOperation(selectedColumn, new SQLite3Select(auxiliaryQuery));
            SQLite3ExpressionBag specificCondition = new SQLite3ExpressionBag(INOperation);

            originalQuery = this.genSelectExpression(selectedTable, specificCondition);
            originalQueryString = SQLite3Visitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            // folded query
            // can not use IN VALUES here, because there is no affinity for the right operand of IN when right operand is a list
            try {
                SQLite3Table t = this.createTemporaryTable(auxiliaryQuery, "intable");
                SQLite3TableReference equivalentTable = new SQLite3TableReference(t);
                INOperation = new InOperation(selectedColumn, equivalentTable);
                specificCondition.updateInnerExpr(INOperation);
                foldedQueryString = SQLite3Visitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } finally {
                dropTemporaryTable("intable");
            }
        }
        // There is not `ANY` and `ALL` operator in SQLite3
        // Row Subquery
        else {
            // original query
            SQLite3Table temporaryTable =  this.genTemporaryTable(auxiliaryQuery, this.tempTableName);
            originalQuery = this.genSelectExpression(temporaryTable, null);
            SQLite3TableAndColumnRef tableAndColumnRef = new SQLite3TableAndColumnRef(temporaryTable);
            SQLite3WithClasure withClasure = new SQLite3WithClasure(tableAndColumnRef, new SQLite3Select(auxiliaryQuery));
            originalQuery.setWithClasure(withClasure);
            originalQueryString = SQLite3Visitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            // folded query
            if (Randomly.getBoolean() && this.testCommonTableExpression()) {
                // there are too many false positives
                // common table expression
                // folded query: WITH table AS VALUES ()
                SQLite3Values values = new SQLite3Values(auxiliaryQueryResult, temporaryTable.getColumns());
                originalQuery.updateWithClasureRight(values);
                foldedQueryString = SQLite3Visitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (Randomly.getBoolean() && this.testDerivedTable()) {
                // derived table
                // folded query: SELECT FROM () AS table
                originalQuery.setWithClasure(null);
                SQLite3TableReference tempTableRef = new SQLite3TableReference(temporaryTable);
                SQLite3Alias alias = new SQLite3Alias(new SQLite3Select(auxiliaryQuery), tempTableRef);
                originalQuery.replaceFromTable(this.tempTableName, alias);
                foldedQueryString = SQLite3Visitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (this.testInsert()){
                // there are too many false positives
                // folded query: CREATE the table and INSERT INTO table subquery
                try {
                    this.createTemporaryTable(auxiliaryQuery, this.tempTableName);
                    originalQuery.setWithClasure(null);
                    foldedQueryString = SQLite3Visitor.asString(originalQuery);
                    foldedResult = getQueryResult(foldedQueryString, state);
                } finally {
                    dropTemporaryTable(this.tempTableName);
                }
            } else {
                throw new IgnoreMeException();
            }
        }
        if (foldedResult == null || originalResult == null) {
            throw new IgnoreMeException();
        }
        if (foldedQueryString.equals(originalQueryString)) {
            throw new IgnoreMeException();
        }
        if (!compareResult(foldedResult, originalResult)) {
            reproducer = null; // TODO
            state.getState().getLocalState().log(auxiliaryQueryString + ";\n" + foldedQueryString + ";\n" + originalQueryString + ";");
            throw new AssertionError(auxiliaryQueryResult.toString() + " " + foldedResult.toString() + " " + originalResult.toString());
        }
    }

    private SQLite3Select genSelectExpression(SQLite3Table tempTable, SQLite3Expression specificCondition) {
        SQLite3Tables randomTables = s.getRandomTableNonEmptyTables();
        if (tempTable != null) {
            randomTables.addTable(tempTable);
        }
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            for (SQLite3Table t : this.tablesFromOuterContext) {
                randomTables.addTable(t);
            }
            if (this.joinsInExpr != null) {
                for (Join j : this.joinsInExpr) {
                    SQLite3Table t = j.getTable();
                    randomTables.removeTable(t);
                }
            }
        }

        List<SQLite3Column> columns = randomTables.getColumns();
        if ((!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) && this.joinsInExpr != null) {
            for (Join j : this.joinsInExpr) {
                SQLite3Table t = j.getTable();
                columns.addAll(t.getColumns());
            }
        }
        gen = new SQLite3ExpressionGenerator(state).setColumns(columns);
        List<SQLite3Table> tables = randomTables.getTables();
        List<Join> joinStatements = new ArrayList<>();
        if ((!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) && this.joinsInExpr != null) {
            joinStatements.addAll(this.joinsInExpr);
            this.joinsInExpr = null;
        }
        else if (Randomly.getBoolean()) {
            joinStatements = genJoinExpression(gen, tables, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, false);
        }
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
        SQLite3Select select = new SQLite3Select();
        select.setFromList(tableRefs);
        if (joinStatements.size() > 0) {
            select.setJoinClauses(joinStatements);
        }
        
        SQLite3Expression randomWhereCondition = gen.generateExpression();
        SQLite3Expression whereCondition = null;
        if (specificCondition != null) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            whereCondition = new SQLite3Expression.Sqlite3BinaryOperation(randomWhereCondition, specificCondition, operator);
        } else {
            whereCondition = randomWhereCondition;
        }
        select.setWhereClause(whereCondition);
        
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(genOrderBysExpression(gen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null));
        }

        if (Randomly.getBoolean()) {
            List<SQLite3Column> selectedColumns = Randomly.nonEmptySubset(columns);
            List<SQLite3Expression> selectedAlias = new LinkedList<>();
            for (int i = 0; i < selectedColumns.size(); ++i) {
                SQLite3ColumnName originalName = new SQLite3ColumnName(selectedColumns.get(i), null);
                SQLite3ColumnName aliasName = new SQLite3ColumnName(SQLite3Column.createDummy("c" + String.valueOf(i)), null);
                SQLite3Alias columnAlias = new SQLite3Alias(originalName, aliasName);
                selectedAlias.add(columnAlias);
            }
            select.setFetchColumns(selectedAlias);
        } else {
            SQLite3ColumnName aggr = new SQLite3ColumnName(Randomly.fromList(columns), null);
            SQLite3Provider.mustKnowResult = true;
            SQLite3Expression originalName = new SQLite3Aggregate(Arrays.asList(aggr), SQLite3Aggregate.SQLite3AggregateFunction.getRandom());
            SQLite3ColumnName aliasName = new SQLite3ColumnName(SQLite3Column.createDummy("c0"), null);
            SQLite3Alias columnAlias = new SQLite3Alias(originalName, aliasName);
            select.setFetchColumns(Arrays.asList(columnAlias));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                List<SQLite3Expression> groupByClause = genGroupByClause(columns, specificCondition);
                select.setGroupByClause(groupByClause);
                if (groupByClause.size() > 0 && Randomly.getBooleanWithRatherLowProbability()) {
                    select.setHavingClause(genHavingClause(columns, specificCondition));
                }
            }
        }
        return select;
    }

    // For expression test
    private SQLite3Select genSimpleSelect(SQLite3Table tempTable, SQLite3Expression specificCondition) {
        SQLite3Tables randomTables = s.getRandomTableNonEmptyTables();
        if (tempTable != null) {
            randomTables.addTable(tempTable);
        }
        if (!useSubqueryAsFoldedExpr) {
            for (SQLite3Table t : this.tablesFromOuterContext) {
                randomTables.addTable(t);
            }
            if (this.joinsInExpr != null) {
                for (Join j : this.joinsInExpr) {
                    SQLite3Table t = j.getTable();
                    randomTables.removeTable(t);
                }
            }
        }

        List<SQLite3Column> columns = randomTables.getColumns();
        if (!useSubqueryAsFoldedExpr && this.joinsInExpr != null) {
            for (Join j : this.joinsInExpr) {
                SQLite3Table t = j.getTable();
                columns.addAll(t.getColumns());
            }
        }
        gen = new SQLite3ExpressionGenerator(state).setColumns(columns);
        List<SQLite3Table> tables = randomTables.getTables();
        tablesFromOuterContext = randomTables.getTables();

        if (joinsInExpr == null) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                joinsInExpr = genJoinExpression(gen, tables, null, true);
            } else {
                joinsInExpr = new ArrayList<Join>();
            }
        }

        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
        SQLite3Select select = new SQLite3Select();
        select.setFromList(tableRefs);
        if (joinsInExpr != null && joinsInExpr.size() > 0) {
            select.setJoinClauses(joinsInExpr);
        }

        SQLite3Expression whereCondition = gen.generateExpression();
        if (specificCondition != null) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            whereCondition = new SQLite3Expression.Sqlite3BinaryOperation(whereCondition, specificCondition, operator);
        }
        this.foldedExpr = whereCondition;

        List<SQLite3Expression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (SQLite3Column c : randomTables.getColumns()) {
            SQLite3ColumnName cRef = new SQLite3ColumnName(c, null);
            SQLite3ColumnName aliasName = new SQLite3ColumnName(SQLite3Column.createDummy("c" + String.valueOf(columnIdx)), null);
            SQLite3Alias columnAlias = new SQLite3Alias(cRef, aliasName);
            fetchColumns.add(columnAlias);
            columnIdx++;
        }

        // add the expression to fetch clause
        SQLite3ColumnName aliasName = new SQLite3ColumnName(SQLite3Column.createDummy("c" + String.valueOf(columnIdx)), null);
        SQLite3Alias columnAlias = new SQLite3Alias(whereCondition, aliasName);
        fetchColumns.add(columnAlias);

        select.setFetchColumns(fetchColumns);

        originalQueryString = SQLite3Visitor.asString(select);

        Map<String, List<SQLite3Constant>> queryRes = null;
        try {
            queryRes = getQueryResult(originalQueryString, state);
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(e.getMessage());
            }
        } 
        if (queryRes.get("c0").size() == 0) {
            throw new IgnoreMeException();
        }

        // save the result first
        selectResult.clear();
        selectResult.putAll(queryRes);

        // get the summary from results
        List<SQLite3Constant> summary = queryRes.remove("c" + String.valueOf(columnIdx));

        List<SQLite3Column> tempColumnList = new ArrayList<>();

        for (int i = 0; i < fetchColumns.size() - 1; ++i) {
            // do not put the last fetch column to values
            SQLite3Alias cAlias = (SQLite3Alias) fetchColumns.get(i);
            SQLite3ColumnName cRef = (SQLite3ColumnName) cAlias.getOrigonalExpression();
            SQLite3Column column = cRef.getColumn();
            String columnName = SQLite3Visitor.asString(cAlias.getAliasExpression());
            SQLite3Column newColumn = new SQLite3Column(columnName, column.getType(), false, false, null);
            tempColumnList.add(newColumn);
        }
        List<SQLite3ColumnName> columnRef = new ArrayList<>();
        for (SQLite3Column c : randomTables.getColumns()) {
            columnRef.add(new SQLite3ColumnName(c, null));
        }
        if (tempColumnList.size() != queryRes.size()) {
            throw new AssertionError();
        }
        SQLite3Values values = new SQLite3Values(queryRes, tempColumnList);
        this.constantResOfFoldedExpr = new SQLite3ResultMap(values, columnRef, summary, null);

        return select;
    }

    private SQLite3Select genSelectWithCorrelatedSubquery(SQLite3Table selectedTable, SQLite3Expression specificCondition) {
        // do not support join now
        this.joinsInExpr = new ArrayList<Join>();

        SQLite3Tables outerQueryRandomTables = s.getRandomTableNonEmptyTables();
        SQLite3Tables innerQueryRandomTables = s.getRandomTableNonEmptyTables();

        if (selectedTable != null) {
            innerQueryRandomTables.addTable(selectedTable);
        }

        List<SQLite3Expression> innerQueryFromTables = new ArrayList<>();
        for (SQLite3Table t : innerQueryRandomTables.getTables()) {
            if (!outerQueryRandomTables.isContained(t)) {
                innerQueryFromTables.add(new SQLite3TableReference(t));
            }
        }
        for (SQLite3Table t : outerQueryRandomTables.getTables()) {
            if (innerQueryRandomTables.isContained(t)) {
                innerQueryRandomTables.removeTable(t);

                List<SQLite3Column> newColumns = new ArrayList<>();
                for (SQLite3Column c : t.getColumns()) {
                    SQLite3Column newColumn = new SQLite3Column(c.getName(), c.getType(), false, null, false);
                    newColumns.add(newColumn);
                }
                SQLite3Table newTable = new SQLite3Table(t.getName() + "a", newColumns, null, true, false, false, false);
                for (SQLite3Column c : newColumns) {
                    c.setTable(newTable);
                }
                innerQueryRandomTables.addTable(newTable);
                
                SQLite3Alias alias = new SQLite3Alias(new SQLite3TableReference(t), new SQLite3TableReference(newTable));
                innerQueryFromTables.add(alias);
            }
        }

        List<SQLite3Column> innerQueryColumns = new ArrayList<>();
        innerQueryColumns.addAll(innerQueryRandomTables.getColumns());
        innerQueryColumns.addAll(outerQueryRandomTables.getColumns());
        gen = new SQLite3ExpressionGenerator(state).setColumns(innerQueryColumns);

        SQLite3Select innerQuery = new SQLite3Select();
        innerQuery.setFromList(innerQueryFromTables);

        SQLite3Expression innerQueryWhereCondition = gen.generateExpression();
        if (specificCondition != null) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            innerQueryWhereCondition = new SQLite3Expression.Sqlite3BinaryOperation(innerQueryWhereCondition, specificCondition, operator);
        }
        innerQuery.setWhereClause(innerQueryWhereCondition);

        // use aggregate function in fetch column
        SQLite3ColumnName innerQueryAggr = new SQLite3ColumnName(Randomly.fromList(innerQueryRandomTables.getColumns()), null);
        SQLite3Provider.mustKnowResult = true;
        SQLite3Expression innerQueryAggrName = new SQLite3Aggregate(Arrays.asList(innerQueryAggr), SQLite3Aggregate.SQLite3AggregateFunction.getRandom());
        innerQuery.setFetchColumns(Arrays.asList(innerQueryAggrName));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<SQLite3Expression> groupByClause = genGroupByClause(innerQueryColumns, specificCondition);
            innerQuery.setGroupByClause(groupByClause);
            if (groupByClause.size() > 0 && Randomly.getBooleanWithRatherLowProbability()) {
                innerQuery.setHavingClause(genHavingClause(innerQueryColumns, specificCondition));
            }
        }

        this.foldedExpr = innerQuery; 


        // outer query
        SQLite3Select outerQuery = new SQLite3Select();
        outerQuery.setFromList(SQLite3Common.getTableRefs(outerQueryRandomTables.getTables(), s));
        tablesFromOuterContext = outerQueryRandomTables.getTables();

        List<SQLite3Expression> outerQueryFetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (SQLite3Column c : outerQueryRandomTables.getColumns()) {
            SQLite3ColumnName cRef = new SQLite3ColumnName(c, null);
            SQLite3ColumnName aliasName = new SQLite3ColumnName(SQLite3Column.createDummy("c" + String.valueOf(columnIdx)), null);
            SQLite3Alias columnAlias = new SQLite3Alias(cRef, aliasName);
            outerQueryFetchColumns.add(columnAlias);
            columnIdx++;
        }

        // add the expression to fetch clause
        SQLite3ColumnName aliasName = new SQLite3ColumnName(SQLite3Column.createDummy("c" + String.valueOf(columnIdx)), null);
        SQLite3Alias columnAlias = new SQLite3Alias(innerQuery, aliasName);
        outerQueryFetchColumns.add(columnAlias);

        outerQuery.setFetchColumns(outerQueryFetchColumns);

        originalQueryString = SQLite3Visitor.asString(outerQuery);

        Map<String, List<SQLite3Constant>> queryRes = null;
        try {
            queryRes = getQueryResult(originalQueryString, state);
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(e.getMessage());
            }
        } 
        if (queryRes.get("c0").size() == 0) {
            throw new IgnoreMeException();
        }

        // save the result first
        selectResult.clear();
        selectResult.putAll(queryRes);

        // get the summary from results
        List<SQLite3Constant> summary = queryRes.remove("c" + String.valueOf(columnIdx));

        List<SQLite3Column> tempColumnList = new ArrayList<>();

        for (int i = 0; i < outerQueryFetchColumns.size() - 1; ++i) {
            // do not put the last fetch column to values
            SQLite3Alias cAlias = (SQLite3Alias) outerQueryFetchColumns.get(i);
            SQLite3ColumnName cRef = (SQLite3ColumnName) cAlias.getOrigonalExpression();
            SQLite3Column column = cRef.getColumn();
            String columnName = SQLite3Visitor.asString(cAlias.getAliasExpression());
            SQLite3Column newColumn = new SQLite3Column(columnName, column.getType(), false, false, null);
            tempColumnList.add(newColumn);
        }
        List<SQLite3ColumnName> columnRef = new ArrayList<>();
        for (SQLite3Column c : outerQueryRandomTables.getColumns()) {
            columnRef.add(new SQLite3ColumnName(c, null));
        }
        if (tempColumnList.size() != queryRes.size()) {
            throw new AssertionError();
        }
        SQLite3Values values = new SQLite3Values(queryRes, tempColumnList);
        this.constantResOfFoldedExpr = new SQLite3ResultMap(values, columnRef, summary, null);

        return outerQuery;
    }

    private List<Join> genJoinExpression(SQLite3ExpressionGenerator gen, List<SQLite3Table> tables, SQLite3Expression specificCondition, boolean joinForExperssion) {
        List<Join> joinStatements = new ArrayList<>();
        if (!state.getDbmsSpecificOptions().testJoins) {
            return joinStatements;
        }
        List<JoinType> options = new ArrayList<>(Arrays.asList(JoinType.values()));
        if (Randomly.getBoolean() && tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1 || joinForExperssion) {
                options.remove(JoinType.NATURAL);
            }
            for (int i = 0; i < nrJoinClauses; i++) {
                SQLite3Expression randomOnCondition = gen.generateExpression();
                SQLite3Expression onCondition = null;
                if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                    BinaryOperator operator = BinaryOperator.getRandomOperator();
                    onCondition = new SQLite3Expression.Sqlite3BinaryOperation(randomOnCondition, specificCondition, operator);
                } else {
                    onCondition = randomOnCondition;
                }

                SQLite3Table table = Randomly.fromList(tables);
                tables.remove(table);
                JoinType selectedOption = Randomly.fromList(options);
                if (selectedOption == JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    onCondition = null;
                }
                Join j = new SQLite3Expression.Join(table, onCondition, selectedOption);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }

    private List<SQLite3Expression> genOrderBysExpression(SQLite3ExpressionGenerator gen, SQLite3Expression specificCondition) {
        List<SQLite3Expression> expressions = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            expressions.add(genOrderingTerm(gen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null));
        }
        return expressions;
    }

    private SQLite3Expression genOrderingTerm(SQLite3ExpressionGenerator gen, SQLite3Expression specificCondition) {
        SQLite3Expression expr = gen.generateExpression();
        if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            expr = new SQLite3Expression.Sqlite3BinaryOperation(expr, specificCondition, operator);
        }
        // COLLATE is potentially already generated
        if (Randomly.getBoolean()) {
            expr = new SQLite3OrderingTerm(expr, Ordering.getRandomValue());
        }
        if (state.getDbmsSpecificOptions().testNullsFirstLast && Randomly.getBoolean()) {
            expr = new SQLite3PostfixText(expr, Randomly.fromOptions(" NULLS FIRST", " NULLS LAST"),
                    null /* expr.getExpectedValue() */) {
                @Override
                public boolean omitBracketsWhenPrinting() {
                    return true;
                }
            };
        }
        return expr;
    }

    private List<SQLite3Expression> genGroupByClause(List<SQLite3Column> columns, SQLite3Expression specificCondition) {
        errors.add("GROUP BY term out of range");
        if (Randomly.getBoolean()) {
            List<SQLite3Expression> collect = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                SQLite3Expression expr = new SQLite3ExpressionGenerator(state).setColumns(columns).generateExpression();
                if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                    BinaryOperator operator = BinaryOperator.getRandomOperator();
                    expr = new SQLite3Expression.Sqlite3BinaryOperation(expr, specificCondition, operator);
                }
                collect.add(expr);
            }
            return collect;
        }
        return Collections.emptyList();
    }

    private SQLite3Expression genHavingClause(List<SQLite3Column> columns, SQLite3Expression specificCondition) {
        SQLite3Expression expr = new SQLite3ExpressionGenerator(state).setColumns(columns).generateExpression();
        if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            expr = new SQLite3Expression.Sqlite3BinaryOperation(expr, specificCondition, operator);
        }
        return expr;
    }

    private Map<String, List<SQLite3Constant>> getQueryResult(String queryString, SQLite3GlobalState state) throws SQLException {
        Map<String, List<SQLite3Constant>> result = new LinkedHashMap<>();
        if (options.logEachSelect()) {
            logger.writeCurrentNoLineBreak(queryString);
        }
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
            stmt.setQueryTimeout(600);
            ResultSet rs = null;
            try {
                rs = stmt.executeQuery(queryString);
                ResultSetMetaData metaData = rs.getMetaData();
                Integer columnCount = metaData.getColumnCount();
                Map<Integer, String> idxNameMap = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    result.put("c" + String.valueOf(i-1), new ArrayList<>());
                    idxNameMap.put(i, "c" + String.valueOf(i-1));
                }

                int resultRows = 0;
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        try {
                            Object value = rs.getObject(i);
                            SQLite3Constant constant;
                            if (rs.wasNull()) {
                                constant = SQLite3Constant.createNullConstant();
                            }

                            else if (value instanceof Integer) {
                                constant = SQLite3Constant.createIntConstant(Long.valueOf((Integer) value));
                            } else if (value instanceof Short) {
                                constant = SQLite3Constant.createIntConstant(Long.valueOf((Short) value));
                            } else if (value instanceof Long) {
                                constant = SQLite3Constant.createIntConstant((Long) value);
                            } 

                            else if (value instanceof Double) {
                                constant = SQLite3Constant.createRealConstant((double) value);
                            } else if (value instanceof Float) {
                                constant = SQLite3Constant.createRealConstant(((Float) value).doubleValue());
                            } else if (value instanceof BigDecimal) {
                                constant = SQLite3Constant.createRealConstant(((BigDecimal) value).doubleValue());
                            } 
                            
                            else if (value instanceof Byte) {
                                constant = SQLite3Constant.createBinaryConstant((byte[]) value);
                            } else if (value instanceof byte[]) {
                                constant = SQLite3Constant.createBinaryConstant((byte[]) value);
                            } else if (value instanceof Boolean) {
                                constant = SQLite3Constant.createBoolean((boolean) value);
                            } else if (value instanceof String) {
                                constant = SQLite3Constant.createTextConstant((String) value);
                            } else if (value == null) {
                                constant = SQLite3Constant.createNullConstant();
                            } else {
                                throw new IgnoreMeException();
                            }
                            List<SQLite3Constant> v = result.get(idxNameMap.get(i));
                            v.add(constant);
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
                            throw new IgnoreMeException();
                        }
                    }
                    ++resultRows;
                    if (resultRows > 100) {
                        throw new IgnoreMeException();
                    }
                }
                Main.nrSuccessfulActions.addAndGet(1);
                rs.close();
            } catch (SQLException e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                if (errors.errorIsExpected(e.getMessage())) {
                    throw new IgnoreMeException();
                } else {
                    state.getState().getLocalState().log(queryString);
                    throw new AssertionError(e.getMessage());
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return result;
    }

    private SQLite3Table genTemporaryTable(SQLite3Select select, String tableName) {
        List<SQLite3Expression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, SQLite3DataType> idxTypeMap = getColumnTypeFromSelect(select);

        List<SQLite3Column> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            SQLite3Column column = new SQLite3Column(columnName, idxTypeMap.get(i), false, false, null);
            databaseColumns.add(column);
        }
        SQLite3Table table = new SQLite3Table(tableName, databaseColumns, null, false, false, false, false);
        for (SQLite3Column c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private SQLite3Table createTemporaryTable(SQLite3Select select, String tableName) throws SQLException {
        String selectString = SQLite3Visitor.asString(select);
        Map<Integer, SQLite3DataType> idxTypeMap = getColumnTypeFromSelect(select);

        Integer columnNumber = idxTypeMap.size();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + tableName + " (");
        for (int i = 0; i < columnNumber; ++i) {
            String columnTypeName = "";
            if (idxTypeMap.get(i) != null) {
                switch (idxTypeMap.get(i)) {
                    case INT:
                    case TEXT:
                    case REAL:
                        columnTypeName = idxTypeMap.get(i).name();
                        break;
                    case BINARY:
                        columnTypeName = "";
                        break;
                    default:
                        columnTypeName = "";
                }
            }
            sb.append("c" + String.valueOf(i) + " " + columnTypeName + ", ");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
        sb.append(");");
        String crateTableString = sb.toString();
        if (options.logEachSelect()) {
            logger.writeCurrent(crateTableString);
        }
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
            try {
                stmt.execute(crateTableString);
                Main.nrSuccessfulActions.addAndGet(1);
            } catch (SQLException e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                throw new IgnoreMeException();
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        StringBuilder sb2 = new StringBuilder();
        sb2.append("INSERT INTO " + tableName + " "+ selectString);
        String insertValueString = sb2.toString();
        if (options.logEachSelect()) {
            logger.writeCurrent(insertValueString);
        }
        stmt = null;
        try {
            stmt = this.con.createStatement();
            try {
                Main.nrSuccessfulActions.addAndGet(1);
                stmt.execute(insertValueString);
            } catch (SQLException e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                throw new IgnoreMeException();
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        List<SQLite3Column> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            SQLite3Column column = new SQLite3Column(columnName, idxTypeMap.get(i), false, false, null);
            databaseColumns.add(column);
        }
        SQLite3Table table = new SQLite3Table(tableName, databaseColumns, null, false, false, false, false);
        for (SQLite3Column c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private void dropTemporaryTable(String tableName) throws SQLException {
        String dropString = "DROP TABLE " + tableName + ";";
        if (options.logEachSelect()) {
            logger.writeCurrent(dropString);
        }
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
            try {
                stmt.execute(dropString);
                Main.nrSuccessfulActions.addAndGet(1);
            } catch (SQLException e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                throw new IgnoreMeException();
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    private boolean compareResult(Map<String, List<SQLite3Constant>> r1, Map<String, List<SQLite3Constant>> r2) {
        if (r1.size() != r2.size()) {
            return false;
        }
        for (Map.Entry < String, List<SQLite3Constant> > entry: r1.entrySet()) {
            String currentKey = entry.getKey();
            if (!r2.containsKey(currentKey)) {
                return false;
            } 
            List<SQLite3Constant> v1= entry.getValue();
            List<SQLite3Constant> v2= r2.get(currentKey);
            if (v1.size() != v2.size()) {
                return false;
            }
            List<String> v1Value = new ArrayList<>(v1.stream().map(c -> c.toString()).collect(Collectors.toList()));
            List<String> v2Value = new ArrayList<>(v2.stream().map(c -> c.toString()).collect(Collectors.toList()));
            Collections.sort(v1Value);
            Collections.sort(v2Value);  
            if (!v1Value.equals(v2Value)) {
                return false;
            }
        }
        return true;
    }

    private Map<Integer, SQLite3DataType> getColumnTypeFromSelect(SQLite3Select select) {
        List<SQLite3Expression> fetchColumns = select.getFetchColumns();
        List<SQLite3Expression> newFetchColumns = new ArrayList<>();
        for(SQLite3Expression column : fetchColumns) {
            newFetchColumns.add(column);
            SQLite3Alias columnAlias = (SQLite3Alias) column;
            SQLite3Expression typeofColumn = new SQLite3Typeof(columnAlias.getOrigonalExpression());
            newFetchColumns.add(typeofColumn);
        }
        SQLite3Select newSelect = new SQLite3Select(select);
        newSelect.setFetchColumns(newFetchColumns);
        Map<String, List<SQLite3Constant>> typeResult = null;
        try {
            typeResult = getQueryResult(SQLite3Visitor.asString(newSelect), state);
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(e.getMessage());
            }
        } 

        if (typeResult == null) {
            throw new IgnoreMeException();
        }
        Map<Integer, SQLite3DataType> idxTypeMap = new HashMap<>();
        for (int i = 0; i * 2 < typeResult.size(); ++i) {
            String columnName = "c" + String.valueOf(i * 2 + 1);
            SQLite3Expression t = typeResult.get(columnName).get(0);
            SQLite3TextConstant tString = (SQLite3TextConstant) t;
            String typeName = tString.asString();
            SQLite3DataType cType = SQLite3DataType.getTypeFromName(typeName);
            idxTypeMap.put(i, cType);
        }

        return idxTypeMap;
    }

    public boolean useSubquery() {
        if (this.state.getDbmsSpecificOptions().coddTestModel.equals("random")) {
            return Randomly.getBoolean();
        } else if (this.state.getDbmsSpecificOptions().coddTestModel.equals("expression")) {
            return false;
        } else if (this.state.getDbmsSpecificOptions().coddTestModel.equals("subquery")) {
            return true;
        } else {
            System.out.printf("Wrong option of --coddtest-model, should be one of: random, expression, subquery");
            System.exit(1);
            return false;
        }
    }

    public boolean useCorrelatedSubquery() {
        return Randomly.getBoolean();
    }

    public boolean testCommonTableExpression() {
        return false;
    }
    public boolean testDerivedTable() {
        return true;
    }
    public boolean testInsert() {
        return false;
    }

    @Override
    public String getLastQueryString() {
        return originalQueryString;
    }

    @Override
    public Reproducer<SQLite3GlobalState> getLastReproducer() {
        return reproducer;
    }
}
