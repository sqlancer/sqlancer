package sqlancer.duckdb.test;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.oracle.CODDTestBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBSchema.DuckDBTables;
import sqlancer.duckdb.ast.DuckDBAlias;
import sqlancer.duckdb.ast.DuckDBBinaryOperator;
import sqlancer.duckdb.ast.DuckDBColumnReference;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBExistsOperator;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBExpressionBag;
import sqlancer.duckdb.ast.DuckDBFunction;
import sqlancer.duckdb.ast.DuckDBInOperator;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.ast.DuckDBTableReference;
import sqlancer.duckdb.ast.DuckDBTypeCast;
import sqlancer.duckdb.ast.DuckDBTypeofNode;
import sqlancer.duckdb.ast.DuckDBValues;
import sqlancer.duckdb.ast.DuckDBWithClause;
import sqlancer.duckdb.ast.DuckDBConstant.DuckDBTextConstant;
import sqlancer.duckdb.ast.DuckDBJoin.OuterType;
import sqlancer.duckdb.ast.DuckDBOrderingTerm;
import sqlancer.duckdb.ast.DuckDBResultMap;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBAggregateFunction;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBBinaryArithmeticOperator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBBinaryComparisonOperator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBBinaryLogicalOperator;


public class DuckDBCODDTestOracle extends CODDTestBase<DuckDBGlobalState> implements TestOracle<DuckDBGlobalState> {

    private final DuckDBSchema s;
    private DuckDBExpressionGenerator gen;
    private Reproducer<DuckDBGlobalState> reproducer;

    private String tempTableName = "temp_table";

    private DuckDBExpression foldedExpr;
    private DuckDBExpression constantResOfFoldedExpr;

    private List<DuckDBTable> tablesFromOuterContext = new ArrayList<>();
    private List<DuckDBJoin> joinsInExpr = null;

    Map<String, List<DuckDBExpression>> auxiliaryQueryResult = new HashMap<>();
    Map<String, List<DuckDBExpression>> selectResult = new HashMap<>();

    Boolean useSubqueryAsFoldedExpr;
    Boolean useCorrelatedSubqueryAsFoldedExpr;

    public DuckDBCODDTestOracle(DuckDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        DuckDBErrors.addExpressionErrors(errors);
        DuckDBErrors.addInsertErrors(errors);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws Exception {
        reproducer = null;

        joinsInExpr = null;
        tablesFromOuterContext.clear();

        useSubqueryAsFoldedExpr = useSubquery();
        useCorrelatedSubqueryAsFoldedExpr = useCorrelatedSubquery();

        DuckDBSelect auxiliaryQuery = null;

        if (useSubqueryAsFoldedExpr) {
            if (useCorrelatedSubqueryAsFoldedExpr) {
                auxiliaryQuery = genSelectWithCorrelatedSubquery();
                auxiliaryQueryString = DuckDBToStringVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult.putAll(selectResult);
            } else {
                auxiliaryQuery = genSelectExpression(null, null);
                auxiliaryQueryString = DuckDBToStringVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult = getQueryResult(auxiliaryQueryString, state);
            }
        } else {
            auxiliaryQuery = genSimpleSelect();
            auxiliaryQueryString = DuckDBToStringVisitor.asString(auxiliaryQuery);
            auxiliaryQueryResult.putAll(selectResult);
        }

        DuckDBSelect originalQuery = null;

        Map<String, List<DuckDBExpression>> foldedResult = null;
        Map<String, List<DuckDBExpression>> originalResult = null;

        // dependent expression
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            DuckDBExpressionBag specificCondition =  new DuckDBExpressionBag(this.foldedExpr);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = DuckDBToStringVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            specificCondition.updateInnerExpr(this.constantResOfFoldedExpr);
            foldedQueryString = DuckDBToStringVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        } 
        // independent expression
        // empty result, put the inner query in (NOT) EXIST
        else if (auxiliaryQueryResult.size() == 0 || auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().iterator().next()).size() == 0) {
            boolean isNegated = Randomly.getBoolean() ? false : true;

            // original query
            DuckDBExistsOperator existExpr = new DuckDBExistsOperator(auxiliaryQuery, isNegated);
            DuckDBExpressionBag specificCondition = new DuckDBExpressionBag(existExpr);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = DuckDBToStringVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            DuckDBExpression equivalentExpr = isNegated ? DuckDBConstant.createBooleanConstant(true) : DuckDBConstant.createBooleanConstant(false);
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = DuckDBToStringVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // Scalar Subquery: 1 column and 1 row, consider the inner query as a constant
        else if (auxiliaryQueryResult.size() == 1 && auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size() == 1 && Randomly.getBoolean()) {
            // float value is inexact, as https://duckdb.org/docs/sql/data_types/numeric#floating-point-types
            String typeName = getColumnTypeFromSelect(auxiliaryQuery).get(0).toString();
            if (typeName.startsWith("FLOAT") || typeName.startsWith("DOUBLE") || typeName.startsWith("REAL")) {
                    throw new IgnoreMeException();
            }
            // original query
            DuckDBExpressionBag specificCondition = new DuckDBExpressionBag(auxiliaryQuery);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = DuckDBToStringVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            DuckDBCompositeDataType constantType = this.getColumnTypeFromSelect(auxiliaryQuery).get(0);
            DuckDBConstant constant = (DuckDBConstant) auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).get(0);
            DuckDBTypeCast equivalentExpr = new DuckDBTypeCast(constant, constantType);
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = DuckDBToStringVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
            }
        // one column
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // float value is inexact, as https://duckdb.org/docs/sql/data_types/numeric#floating-point-types
            DuckDBCompositeDataType valuesType = getColumnTypeFromSelect(auxiliaryQuery).get(0);
            String typeName = valuesType.toString();
            if (typeName.startsWith("FLOAT") || typeName.startsWith("DOUBLE") || typeName.startsWith("REAL")) {
                throw new IgnoreMeException();
            }
            // original query
            List<DuckDBColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            DuckDBColumnReference selectedColumn = new DuckDBColumnReference(Randomly.fromList(columns));
            DuckDBTable selectedTable = selectedColumn.getColumn().getTable();
            DuckDBTableReference selectedTableRef = new DuckDBTableReference(selectedTable);
            DuckDBExpressionBag tableBag = new DuckDBExpressionBag(selectedTableRef);

            Boolean isNegatedIn = Randomly.getBoolean();
            DuckDBTypeCast columnWithType = new DuckDBTypeCast(selectedColumn, valuesType);
            DuckDBInOperator optInOperation = new DuckDBInOperator(columnWithType, Arrays.asList(auxiliaryQuery), isNegatedIn);
            DuckDBExpressionBag specificCondition = new DuckDBExpressionBag(optInOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition);
            originalQueryString = DuckDBToStringVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            List<DuckDBExpression> values = new ArrayList<>();
            Map<Integer, DuckDBCompositeDataType> typeList = this.getColumnTypeFromSelect(auxiliaryQuery);
            for (int i = 0; i < auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size(); ++i) {
                List<DuckDBExpression> rowRs = new ArrayList<>();
                for (int j = 0; j < typeList.size(); ++j) {
                    DuckDBConstant c = (DuckDBConstant) auxiliaryQueryResult.get("c" + String.valueOf(j)).get(i);
                    rowRs.add(new DuckDBTypeCast(c, typeList.get(j)));
                }
                DuckDBValues valueRow = new DuckDBValues(rowRs);
                values.add(valueRow);
            }

            DuckDBInOperator refInOperation = new DuckDBInOperator(columnWithType, values, isNegatedIn);
            specificCondition.updateInnerExpr(refInOperation);
            foldedQueryString = DuckDBToStringVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // Row Subquery
        else {
            // original query
            DuckDBTable temporaryTable =  this.genTemporaryTable(auxiliaryQuery, this.tempTableName);
            DuckDBTableReference tableRef = new DuckDBTableReference(temporaryTable);
            DuckDBExpressionBag tableBag = new DuckDBExpressionBag(tableRef);
            originalQuery = this.genSelectExpression(tableBag, null);

            DuckDBWithClause optWithClause = new DuckDBWithClause(tableRef, Arrays.asList(auxiliaryQuery));
            originalQuery.setWithClause(optWithClause);
            originalQueryString = DuckDBToStringVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            if (Randomly.getBoolean()) {
                // folded query: WITH table AS VALUES ()
                List<DuckDBExpression> values = new ArrayList<>();
                Map<Integer, DuckDBCompositeDataType> typeList = this.getColumnTypeFromSelect(auxiliaryQuery);
                for (int i = 0; i < auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size(); ++i){
                    List<DuckDBExpression> rowRs = new ArrayList<>();
                    for (int j = 0; j < typeList.size(); ++j) {
                        DuckDBConstant c = (DuckDBConstant) auxiliaryQueryResult.get("c" + String.valueOf(j)).get(i);
                        rowRs.add(new DuckDBTypeCast(c, typeList.get(j)));
                    }
                    DuckDBValues valueRow = new DuckDBValues(rowRs);
                    values.add(valueRow);
                }
                DuckDBWithClause refWithClause = new DuckDBWithClause(tableRef, values);
                originalQuery.setWithClause(refWithClause);
                foldedQueryString = DuckDBToStringVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (Randomly.getBoolean()) {
                // folded query: SELECT FROM () AS table
                originalQuery.setWithClause(null);
                DuckDBAlias alias = new DuckDBAlias(auxiliaryQuery, DuckDBToStringVisitor.asString(tableRef));
                tableBag.updateInnerExpr(alias);
                foldedQueryString = DuckDBToStringVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else {
                // folded query: CREATE the table
                try {
                    this.createTemporaryTable(auxiliaryQuery, this.tempTableName);
                    originalQuery.setWithClause(null);
                    foldedQueryString = DuckDBToStringVisitor.asString(originalQuery);
                    foldedResult = getQueryResult(foldedQueryString, state);
                } finally {
                    dropTemporaryTable(this.tempTableName);
                }
            }
        }

        if (foldedResult == null || originalResult == null) {
            throw new IgnoreMeException();
        }

        if (foldedQueryString.equals(originalQueryString)) {
            throw new IgnoreMeException();
        }

        if (!this.compareResult(foldedResult, originalResult)) {
            reproducer = null;
            state.getState().getLocalState().log(auxiliaryQueryString + ";\n" + foldedQueryString + ";\n" + originalQueryString + ";");
            throw new AssertionError(auxiliaryQueryResult.toString() + " " + foldedResult.toString() + " " + originalResult.toString());
        }
        
    }

    private DuckDBSelect genSelectExpression(DuckDBExpressionBag tableBag, DuckDBExpression specificCondition) {
        DuckDBTables randomTables = s.getRandomTableNonEmptyTables();
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            for (DuckDBTable t : this.tablesFromOuterContext) {
                randomTables.addTable(t);
            }
            if (this.joinsInExpr != null) {
                for (DuckDBJoin j : this.joinsInExpr) {
                    DuckDBTable t = j.getRightTable().getTable();
                    randomTables.removeTable(t);
                }
                for (DuckDBJoin j : this.joinsInExpr) {
                    DuckDBTable t = j.getLeftTable().getTable();
                    randomTables.removeTable(t);
                }
            }
        }
        DuckDBTable tempTable = null;
        DuckDBTableReference tableRef = null;
        if (tableBag != null) {
            tableRef = (DuckDBTableReference) tableBag.getInnerExpr();
            tempTable = tableRef.getTable();
            // randomTables.addTable(tempTable);
        }
        List<DuckDBColumn> columns = randomTables.getColumns();
        if (tempTable != null) {
            columns.addAll(tempTable.getColumns());
        }
        if ((!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) && this.joinsInExpr != null) {
            for (DuckDBJoin j : this.joinsInExpr) {
                DuckDBTable t = j.getRightTable().getTable();
                columns.addAll(t.getColumns());
                t = j.getLeftTable().getTable();
                columns.addAll(t.getColumns());
            }
        }
        gen = new DuckDBExpressionGenerator(state).setColumns(columns);
        List<DuckDBTable> tables = randomTables.getTables();
        List<DuckDBTableReference> tableRefList = tables.stream()
                .map(t -> new DuckDBTableReference(t)).collect(Collectors.toList());

        DuckDBSelect select = new DuckDBSelect();

        List<DuckDBExpression> joins = new ArrayList<>();
        if ((!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr))) {
            if (this.joinsInExpr != null) {
                joins.addAll(this.joinsInExpr);
                this.joinsInExpr = null;
            }
        }
        else if (Randomly.getBoolean()) {
            joins = genJoinExpression(tableRefList, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null);
        }
        if (joins.size() > 0) {
            select.setJoinList(joins);
        }
        
        select.setFromList(tableRefList.stream().collect(Collectors.toList()));
        if (tableBag != null && !tables.contains(tempTable)) {
            select.addToFromList(tableBag);
        }

        DuckDBExpression randomWhereCondition = gen.generateExpression();
        if (specificCondition != null) {
            Operator operator = Randomly.fromList(Arrays.asList(DuckDBBinaryComparisonOperator.getRandom(), DuckDBBinaryArithmeticOperator.getRandom(), DuckDBBinaryLogicalOperator.getRandom()));
            randomWhereCondition = new DuckDBBinaryOperator(randomWhereCondition, specificCondition, operator);
        } 
        select.setWhereClause(randomWhereCondition);
        
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(genOrderBys(specificCondition));
        }

        if (Randomly.getBoolean()) {
            List<DuckDBColumn> selectedColumns = Randomly.nonEmptySubset(columns);
            List<DuckDBExpression> selectedAlias = new ArrayList<>();
            for (int i = 0; i < selectedColumns.size(); ++i) {
                DuckDBColumnReference originalName = new DuckDBColumnReference(selectedColumns.get(i));
                DuckDBAlias columnAlias = new DuckDBAlias(originalName, "c" + String.valueOf(i));
                selectedAlias.add(columnAlias);
            }
            select.setFetchColumns(selectedAlias);
        } else {
            DuckDBColumnReference columnName = new DuckDBColumnReference(Randomly.fromList(columns));
            DuckDBAggregateFunction aggregateFunction = Randomly.fromOptions(DuckDBAggregateFunction.MAX,
                DuckDBAggregateFunction.MIN, DuckDBAggregateFunction.SUM, DuckDBAggregateFunction.COUNT,
                DuckDBAggregateFunction.AVG);
            DuckDBFunction<DuckDBAggregateFunction> aggregate = new DuckDBFunction<>(Arrays.asList(columnName), aggregateFunction);
            DuckDBAlias columnAlias = new DuckDBAlias(aggregate, "c0");
            select.setFetchColumns(Arrays.asList(columnAlias));
            select.setGroupByExpressions(genGroupBys(Randomly.nonEmptySubset(columns), specificCondition));
            // gen having
            if (Randomly.getBooleanWithRatherLowProbability()) {
                DuckDBExpressionGenerator havingGen = new DuckDBExpressionGenerator(state).setColumns(columns);
                DuckDBExpression havingExpr = havingGen.generateExpression();
                if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                    Operator operator = Randomly.fromList(Arrays.asList(DuckDBBinaryComparisonOperator.getRandom(),     DuckDBBinaryArithmeticOperator.getRandom(), DuckDBBinaryLogicalOperator.getRandom()));
                    havingExpr = new DuckDBBinaryOperator(havingExpr, specificCondition, operator);
                }
                select.setHavingClause(havingExpr);
            }
        }
        return select;
    }

    private DuckDBSelect genSelectWithCorrelatedSubquery() {
        DuckDBTables outerQueryRandomTables = s.getRandomTableNonEmptyTables();
        DuckDBTables innerQueryRandomTables = s.getRandomTableNonEmptyTables();

        List<DuckDBExpression> innerQueryFromTables = new ArrayList<>();
        for (DuckDBTable t : innerQueryRandomTables.getTables()) {
            if (!outerQueryRandomTables.isContained(t))
                innerQueryFromTables.add(new DuckDBTableReference(t));
        }
        if (innerQueryFromTables.size() == 0) {
            throw new IgnoreMeException();
        }

        // for (DuckDBTable t : outerQueryRandomTables.getTables()) {
        //     if (!innerQueryRandomTables.isContained(t)) {
        //         tablesFromOuterContext.add(t);
        //     }
        // }

        List<DuckDBColumn> innerQueryColumns = new ArrayList<>();
        innerQueryColumns.addAll(innerQueryRandomTables.getColumns());
        innerQueryColumns.addAll(outerQueryRandomTables.getColumns());
        gen = new DuckDBExpressionGenerator(state).setColumns(innerQueryColumns);

        DuckDBSelect innerQuery = new DuckDBSelect();
        innerQuery.setFromList(innerQueryFromTables);

        DuckDBExpression innerQueryWhereCondition = gen.generateExpression();
        innerQuery.setWhereClause(innerQueryWhereCondition);

        // use aggregate function in fetch column
        DuckDBColumnReference innerQueryAggrColumn = new DuckDBColumnReference(Randomly.fromList(innerQueryRandomTables.getColumns()));
        DuckDBAggregateFunction aggregateFunction = Randomly.fromOptions(DuckDBAggregateFunction.MAX,
                DuckDBAggregateFunction.MIN, DuckDBAggregateFunction.SUM, DuckDBAggregateFunction.COUNT,
                DuckDBAggregateFunction.AVG);
        DuckDBFunction<DuckDBAggregateFunction> aggregate = new DuckDBFunction<>(Arrays.asList(innerQueryAggrColumn), aggregateFunction);
        innerQuery.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<DuckDBExpression> groupByClause = genGroupBys(innerQueryColumns, null);
            innerQuery.setGroupByClause(groupByClause);
            if (groupByClause.size() > 0 && Randomly.getBooleanWithRatherLowProbability()) {
                DuckDBExpressionGenerator havingGen = new DuckDBExpressionGenerator(state).setColumns(innerQueryRandomTables.getColumns());
                DuckDBExpression havingExpr = havingGen.generateExpression();
                innerQuery.setHavingClause(havingExpr);
            }
        }

        this.foldedExpr = innerQuery; 


        // outer query
        DuckDBSelect outerQuery = new DuckDBSelect();
        List<DuckDBExpression> tableRefList = outerQueryRandomTables.getTables().stream()
                .map(t -> new DuckDBTableReference(t)).collect(Collectors.toList());
        outerQuery.setFromList(tableRefList);
        tablesFromOuterContext = outerQueryRandomTables.getTables();

        List<DuckDBExpression> outerQueryFetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (DuckDBColumn c : outerQueryRandomTables.getColumns()) {
            DuckDBColumnReference cRef = new DuckDBColumnReference(c);
            String aliasName = "c" + String.valueOf(columnIdx);
            DuckDBAlias columnAlias = new DuckDBAlias(cRef, aliasName);
            outerQueryFetchColumns.add(columnAlias);
            columnIdx++;
        }

        // add the expression to fetch clause
        String aliasName = "c" + String.valueOf(columnIdx);
        DuckDBAlias columnAlias = new DuckDBAlias(innerQuery, aliasName);
        outerQueryFetchColumns.add(columnAlias);

        outerQuery.setFetchColumns(outerQueryFetchColumns);

        originalQueryString = DuckDBToStringVisitor.asString(outerQuery);

        Map<String, List<DuckDBExpression>> queryRes = null;
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

        // get the constant corresponding to each row from results
        List<DuckDBExpression> summary = queryRes.remove("c" + String.valueOf(columnIdx));
        Map<Integer, DuckDBCompositeDataType> columnsType = getColumnTypeFromSelect(outerQuery);

        DuckDBCompositeDataType exprType = columnsType.get(outerQueryFetchColumns.size() -1 );
        if (exprType.toString().equals("REAL") || exprType.toString().startsWith("FLOAT")) {
            throw new IgnoreMeException();
        }

        List<DuckDBExpression> constantRes = new ArrayList<>();
        for (DuckDBExpression e : summary) {
            DuckDBConstant c = (DuckDBConstant) e;
            constantRes.add(new DuckDBTypeCast(c, exprType));
        }

        LinkedHashMap<DuckDBColumnReference, List<DuckDBExpression>> dbstate = new LinkedHashMap<>();

        for (int i = 0; i < outerQueryFetchColumns.size() - 1; ++i) {
            // do not put the last fetch column to values
            DuckDBAlias cAlias = (DuckDBAlias) outerQueryFetchColumns.get(i);
            DuckDBColumnReference cRef = (DuckDBColumnReference) cAlias.getExpr();
            String columnName = cAlias.getAlias();
            List<DuckDBExpression> constants = new ArrayList<>();
            for (DuckDBExpression e : queryRes.get(columnName)) {
                DuckDBConstant c = (DuckDBConstant) e;
                if (columnsType.get(i).toString().equals("REAL") || columnsType.get(i).toString().startsWith("FLOAT")) {
                    throw new IgnoreMeException();
                }
                constants.add(new DuckDBTypeCast(c, columnsType.get(i)));
            }
            dbstate.put(cRef, constants);
        }
        this.constantResOfFoldedExpr = new DuckDBResultMap(dbstate, constantRes);

        return outerQuery;
    }

    private DuckDBSelect genSimpleSelect() {
        DuckDBTables randomTables = s.getRandomTableNonEmptyTables();
        List<DuckDBColumn> columns = randomTables.getColumns();
        gen = new DuckDBExpressionGenerator(state).setColumns(columns);
        List<DuckDBTable> tables = randomTables.getTables();
        tablesFromOuterContext = randomTables.getTables();
        List<DuckDBTableReference> tableRefList = tables.stream()
                .map(t -> new DuckDBTableReference(t)).collect(Collectors.toList());

        DuckDBSelect select = new DuckDBSelect();

        if (Randomly.getBoolean()) {
            List<DuckDBExpression> joins = genJoinExpression(tableRefList, null);
            select.setJoinList(joins);
            this.joinsInExpr = joins.stream()
                .map(expr -> (DuckDBJoin) expr)
                .collect(Collectors.toList());
        }
        
        select.setFromList(tableRefList.stream().collect(Collectors.toList()));

        DuckDBExpression randomWhereCondition = gen.generateExpression();
        this.foldedExpr = randomWhereCondition;
        
        List<DuckDBExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (DuckDBColumn c : randomTables.getColumns()) {
            DuckDBColumnReference cRef = new DuckDBColumnReference(c);
            String aliasName = "c" + String.valueOf(columnIdx);
            DuckDBAlias columnAlias = new DuckDBAlias(cRef, aliasName);
            fetchColumns.add(columnAlias);
            columnIdx++;
        }

        // add the expression to fetch clause
        String exprAliasName = "c" + String.valueOf(columnIdx);
        DuckDBAlias exprAlias = new DuckDBAlias(randomWhereCondition, exprAliasName);
        fetchColumns.add(exprAlias);

        select.setFetchColumns(fetchColumns);

        Map<String, List<DuckDBExpression>> queryRes = null;
        try {
            queryRes = getQueryResult(DuckDBToStringVisitor.asString(select), state);
        } catch (SQLException e){
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

        // get the constant corresponding to each row from results
        List<DuckDBExpression> summary = queryRes.remove(exprAliasName);
        Map<Integer, DuckDBCompositeDataType> columnsType = getColumnTypeFromSelect(select);
        DuckDBCompositeDataType exprType = columnsType.get(fetchColumns.size() -1 );
        if (exprType.toString().equals("REAL") || exprType.toString().startsWith("FLOAT")) {
            throw new IgnoreMeException();
        }
        List<DuckDBExpression> constantRes = new ArrayList<>();
        for (DuckDBExpression e : summary) {
            DuckDBConstant c = (DuckDBConstant) e;
            constantRes.add(new DuckDBTypeCast(c, exprType));
        }

        LinkedHashMap<DuckDBColumnReference, List<DuckDBExpression>> dbstate = new LinkedHashMap<>();

        for (int i = 0; i < fetchColumns.size() - 1; ++i) {
            // do not put the last fetch column to values
            DuckDBAlias cAlias = (DuckDBAlias) fetchColumns.get(i);
            DuckDBColumnReference cRef = (DuckDBColumnReference) cAlias.getExpr();
            String columnName = cAlias.getAlias();
            List<DuckDBExpression> constants = new ArrayList<>();
            for (DuckDBExpression e : queryRes.get(columnName)) {
                DuckDBConstant c = (DuckDBConstant) e;
                if (columnsType.get(i).toString().equals("REAL") || columnsType.get(i).toString().startsWith("FLOAT")) {
                    throw new IgnoreMeException();
                }
                constants.add(new DuckDBTypeCast(c, columnsType.get(i)));
            }
            dbstate.put(cRef, constants);
        }
        this.constantResOfFoldedExpr = new DuckDBResultMap(dbstate, constantRes);

        return select;
    }

    private List<DuckDBExpression> genJoinExpression(
        List<DuckDBTableReference> tableList, DuckDBExpression specificCondition) {
        List<DuckDBExpression> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            DuckDBTableReference leftTable = tableList.remove(0);
            DuckDBTableReference rightTable = tableList.remove(0);
            List<DuckDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            DuckDBExpressionGenerator joinGen = new DuckDBExpressionGenerator(state).setColumns(columns);
            DuckDBExpression onPredicate = joinGen.generateExpression();
            if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                Operator operator = Randomly.fromList(Arrays.asList(DuckDBBinaryComparisonOperator.getRandom(), DuckDBBinaryArithmeticOperator.getRandom(), DuckDBBinaryLogicalOperator.getRandom()));
                onPredicate = new DuckDBBinaryOperator(onPredicate, specificCondition, operator);
            } 
            
            switch (DuckDBJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(DuckDBJoin.createInnerJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case NATURAL:
                joinExpressions.add(DuckDBJoin.createNaturalJoin(leftTable, rightTable, OuterType.getRandom()));
                break;
            case LEFT:
                joinExpressions
                        .add(DuckDBJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case RIGHT:
                joinExpressions
                        .add(DuckDBJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    private List<DuckDBExpression> genOrderBys(DuckDBExpression specificCondition) {
        List<DuckDBExpression> expr = gen.generateExpressions(Randomly.smallNumber() + 1);
        List<DuckDBExpression> newExpr = new ArrayList<>(expr.size());
        for (DuckDBExpression curExpr : expr) {
            if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                Operator operator = Randomly.fromList(Arrays.asList(DuckDBBinaryComparisonOperator.getRandom(), DuckDBBinaryArithmeticOperator.getRandom(), DuckDBBinaryLogicalOperator.getRandom()));
                curExpr = new DuckDBBinaryOperator(curExpr, specificCondition, operator);
            }
            if (Randomly.getBoolean()) {
                curExpr = new DuckDBOrderingTerm(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    private List<DuckDBExpression> genGroupBys(List<DuckDBColumn> columns, DuckDBExpression specificCondition) {
        DuckDBExpressionGenerator groupByGen = new DuckDBExpressionGenerator(state).setColumns(columns);
        List<DuckDBExpression> exprs = groupByGen.generateGroupBys();
        List<DuckDBExpression> newExpr = new ArrayList<>(exprs.size());
        for (DuckDBExpression curExpr : exprs) {
            if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                Operator operator = Randomly.fromList(Arrays.asList(DuckDBBinaryComparisonOperator.getRandom(), DuckDBBinaryArithmeticOperator.getRandom(), DuckDBBinaryLogicalOperator.getRandom()));
                curExpr = new DuckDBBinaryOperator(curExpr, specificCondition, operator);
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    }
    
    private Map<String, List<DuckDBExpression>> getQueryResult(String queryString, DuckDBGlobalState state) throws SQLException {
        Map<String, List<DuckDBExpression>> result = new LinkedHashMap<>();
        if (options.logEachSelect()) {
            logger.writeCurrent(queryString);
        }
        try (Statement s = this.con.createStatement()) {
            try (ResultSet rs = s.executeQuery(queryString)) {
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
                            DuckDBExpression constant;
                            if (rs.wasNull()) {
                                constant = DuckDBConstant.createNullConstant();
                            } else if (value instanceof Integer) {
                                constant = DuckDBConstant.createIntConstant(BigInteger.valueOf((Integer) value));
                            } else if (value instanceof Short) {
                                constant = DuckDBConstant.createIntConstant(BigInteger.valueOf((Short) value));
                            } else if (value instanceof BigInteger) {
                                constant = DuckDBConstant.createIntConstant((BigInteger) value);
                            } else if (value instanceof Long) {
                                constant = DuckDBConstant.createIntConstant(BigInteger.valueOf((long) value));
                            } else if (value instanceof Double) {
                                constant = DuckDBConstant.createFloatConstant((double) value);
                            } else if (value instanceof Float) {
                                constant = DuckDBConstant.createFloatConstant(Double.valueOf((Float)value));
                            } else if (value instanceof Boolean) {
                                constant = DuckDBConstant.createBooleanConstant((Boolean) value);
                            }else if (value instanceof java.sql.Timestamp) {
                                constant = DuckDBConstant.createTimestampConstant(((java.sql.Timestamp) value).getTime());
                            } else if (value instanceof java.sql.Date) {
                                constant = DuckDBConstant.createDateConstant(((java.sql.Date) value).getTime());
                            } 
                            else if (value instanceof String) {
                                constant = DuckDBConstant.createStringConstant((String) value);
                            } else {
                                throw new IgnoreMeException();
                            }
                            List<DuckDBExpression> v = result.get(idxNameMap.get(i));
                            v.add(constant);
                        } catch (Exception e) {
                            if (errors.errorIsExpected(e.getMessage())) {
                                throw new IgnoreMeException();
                            } else {
                                throw new AssertionError(e.getMessage());
                            }
                        }
                    }
                    ++resultRows;
                    if (resultRows > 100) {
                        throw new IgnoreMeException();
                    }
                }
                rs.close();
                Main.nrSuccessfulActions.addAndGet(1);
            } catch (Exception e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                if (e.getMessage() == null || errors.errorIsExpected(e.getMessage())) {
                    throw new IgnoreMeException();
                } else {
                    state.getState().getLocalState().log(queryString);
                    throw new AssertionError(e.getMessage());
                }
            }
        } 
        return result;
    }

    private DuckDBTable genTemporaryTable(DuckDBSelect select, String tableName) {
        int columnNumber = select.getFetchColumns().size();
        Map<Integer, DuckDBCompositeDataType> idxTypeMap = this.getColumnTypeFromSelect(select);
        
        List<DuckDBColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            DuckDBColumn column = new DuckDBColumn(columnName, idxTypeMap.get(i), false, false);
            databaseColumns.add(column);
        }
        DuckDBTable table = new DuckDBTable(tableName, databaseColumns, false);
        for (DuckDBColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private DuckDBTable createTemporaryTable(DuckDBSelect select, String tableName) throws SQLException {
        String selectString = DuckDBToStringVisitor.asString(select);
        Integer columnNumber = select.getFetchColumns().size();

        Map<Integer, DuckDBCompositeDataType> idxTypeMap = this.getColumnTypeFromSelect(select);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + tableName + " (");
        for (int i = 0; i < columnNumber; ++i) {
            String columnTypeName = idxTypeMap.get(i).toString();
            sb.append("c" + String.valueOf(i) + " " + columnTypeName);
            if (i < columnNumber - 1) {
                sb.append(", ");
            }
        }
        sb.append(");");
        String crateTableString = sb.toString();
        if (options.logEachSelect()) {
            logger.writeCurrent(crateTableString);
        }
        try (Statement s = this.con.createStatement()) {
            try {
                s.execute(crateTableString);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        }

        StringBuilder sb2 = new StringBuilder();
        sb2.append("INSERT INTO " + tableName + " "+ selectString);
        String insertValueString = sb2.toString();
        if (options.logEachSelect()) {
            logger.writeCurrent(insertValueString);
        }
        try (Statement s = this.con.createStatement()) {
            try {
                s.execute(insertValueString);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        }

        List<DuckDBColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            DuckDBColumn column = new DuckDBColumn(columnName, idxTypeMap.get(i), false, false);
            databaseColumns.add(column);
        }
        DuckDBTable table = new DuckDBTable(tableName, databaseColumns, false);
        for (DuckDBColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private void dropTemporaryTable(String tableName) throws SQLException {
        String dropString = "DROP TABLE " + tableName + ";";
        if (options.logEachSelect()) {
            logger.writeCurrent(dropString);
        }
        try (Statement s = this.con.createStatement()) {
            try {
                s.execute(dropString);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        }
    }

    private Map<Integer, DuckDBCompositeDataType> getColumnTypeFromSelect(DuckDBSelect select) {
        DuckDBSelect newSelect = new DuckDBSelect(select);


        List<DuckDBExpression> fetchColumns = select.getFetchColumns();
        List<DuckDBExpression> newFetchColumns = new ArrayList<>();

        DuckDBSelect innerQuery = null;
        DuckDBExpression innerQueryFetchColumn = null;

        for(DuckDBExpression column : fetchColumns) {
            DuckDBAlias columnAlias = (DuckDBAlias) column;
            DuckDBExpression columnExpr = columnAlias.getExpr();
            if (columnExpr instanceof DuckDBSelect) {
                innerQuery = (DuckDBSelect) columnExpr;
                List<DuckDBExpression> innerQueryFetchColumns = innerQuery.getFetchColumns();
                List<DuckDBExpression> innerQueryNewFetchColumns = new ArrayList<>();
                for (DuckDBExpression innerQueryColumn : innerQueryFetchColumns) {
                    innerQueryFetchColumn = innerQueryColumn;
                    DuckDBExpression typeofColumnForInnerQuery = new DuckDBTypeofNode(innerQueryColumn);
                    innerQueryNewFetchColumns.add(typeofColumnForInnerQuery);
                }
                innerQuery.setFetchColumns(innerQueryNewFetchColumns);
                DuckDBAlias subqueryTypeAlias = new DuckDBAlias(innerQuery, "innerType");
                newFetchColumns.add(subqueryTypeAlias);
            }
            else {
                DuckDBTypeofNode typeofColumn = new DuckDBTypeofNode(columnAlias.getExpr());
                newFetchColumns.add(typeofColumn);
            }
        }
        
        newSelect.setFetchColumns(newFetchColumns);
        Map<String, List<DuckDBExpression>> typeResult = null;
        try {
            typeResult = getQueryResult(DuckDBToStringVisitor.asString(newSelect), state);
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
        Map<Integer, DuckDBCompositeDataType> idxTypeMap = new HashMap<>();
        for (int i = 0; i < typeResult.size(); ++i) {
            String columnName = "c" + String.valueOf(i);
            DuckDBExpression t = typeResult.get(columnName).get(0);
            String typeName = "";
            if (t instanceof DuckDBTextConstant) {
                DuckDBTextConstant tString = (DuckDBTextConstant) t;
                typeName = tString.getValue();
            } else {
                typeName = "unknown";
            }
            DuckDBCompositeDataType cType = new DuckDBCompositeDataType(typeName);
            idxTypeMap.put(i, cType);
        }

        // revert innerquery
        if (innerQuery != null) {
            innerQuery.setFetchColumns(Arrays.asList(innerQueryFetchColumn));
        }
        
        for (Map.Entry<Integer, DuckDBCompositeDataType> entry: idxTypeMap.entrySet()) {
            Integer index = entry.getKey();
            String typeName = entry.getValue().toString();
            if (typeName.equals("unknown")) {
                DuckDBExpression column = fetchColumns.get(index);
                DuckDBCompositeDataType columnType = null;
                if (column instanceof DuckDBColumnReference) {
                    DuckDBColumnReference c = (DuckDBColumnReference)column;
                    columnType = c.getColumn().getType();
                } else if (column instanceof DuckDBAlias) {
                    DuckDBAlias a = (DuckDBAlias) column;
                    DuckDBExpression left = a.getExpr();
                    if (left instanceof DuckDBColumnReference) {
                        DuckDBColumnReference c = (DuckDBColumnReference) left;
                        columnType = c.getColumn().getType();
                    } else if (left instanceof DuckDBFunction) {
                        DuckDBFunction<DuckDBAggregateFunction> aggr = (DuckDBFunction<DuckDBAggregateFunction>) left;
                        List<DuckDBExpression> aggrExprs =  aggr.getArgs();
                        DuckDBExpression aggrExpr = aggrExprs.get(0);
                        if (aggrExpr instanceof DuckDBColumnReference) {
                            DuckDBColumnReference c = (DuckDBColumnReference) aggrExpr;
                            columnType = c.getColumn().getType();
                        } 
                    } else if (left instanceof DuckDBSelect) {
                        DuckDBSelect sub = (DuckDBSelect) left;
                        DuckDBExpression subFetchColumn = sub.getFetchColumns().get(0);
                        if (subFetchColumn instanceof DuckDBFunction) {
                            DuckDBFunction<DuckDBAggregateFunction> subAggr = (DuckDBFunction<DuckDBAggregateFunction>) subFetchColumn;
                            DuckDBExpression cr = subAggr.getArgs().get(0);
                            if (cr instanceof DuckDBColumnReference) {
                                columnType = ((DuckDBColumnReference) cr).getColumn().getType();
                            }
                        }
                    }
                } 
                if (columnType != null) {
                    idxTypeMap.put(index, columnType);
                }
            }
        }
        return idxTypeMap;
    }

    private boolean compareResult(Map<String, List<DuckDBExpression>> r1, Map<String, List<DuckDBExpression>> r2) {
        if (r1.size() != r2.size()) {
            return false;
        }
        for (Map.Entry < String, List<DuckDBExpression> > entry: r1.entrySet()) {
            String currentKey = entry.getKey();
            if (!r2.containsKey(currentKey)) {
                return false;
            } 
            List<DuckDBExpression> v1= entry.getValue();
            List<DuckDBExpression> v2= r2.get(currentKey);
            if (v1.size() != v2.size()) {
                return false;
            }
            List<String> v1Value = new ArrayList<>(v1.stream().map(c -> ((DuckDBConstant)c).toString()).collect(Collectors.toList()));
            List<String> v2Value = new ArrayList<>(v2.stream().map(c -> ((DuckDBConstant)c).toString()).collect(Collectors.toList()));
            Collections.sort(v1Value);
            Collections.sort(v2Value);  
            // if (!v1Value.equals(v2Value)) {
            //     return false;
            // }
            for (int i = 0; i < v1Value.size(); ++i) {
                if (!v1Value.get(i).equals(v2Value.get(i))) {
                    String regx = "[+-]*\\d+\\.?\\d*[Ee]*[+-]*\\d+";
                    Pattern pattern = Pattern.compile(regx);
                    if (pattern.matcher(v1Value.get(i)).matches() && pattern.matcher(v2Value.get(i)).matches()) {
                        if (!v1Value.get(i).substring(0, 6).equals(v2Value.get(i).substring(0, 6))) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
        return true;
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

    @Override
    public String getLastQueryString() {
        return originalQueryString;
    }

    @Override
    public Reproducer<DuckDBGlobalState> getLastReproducer() {
        return reproducer;
    }
}
