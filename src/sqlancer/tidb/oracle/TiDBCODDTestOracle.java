package sqlancer.tidb.oracle;

import java.math.BigDecimal;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.CODDTestBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBCompositeDataType;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.TiDBSchema.TiDBTables;
import sqlancer.tidb.ast.TiDBAggregate;
import sqlancer.tidb.ast.TiDBAlias;
import sqlancer.tidb.ast.TiDBAllOperator;
import sqlancer.tidb.ast.TiDBAnyOperator;
import sqlancer.tidb.ast.TiDBBinaryComparisonOperation;
import sqlancer.tidb.ast.TiDBBinaryLogicalOperation;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBExists;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBExpressionBag;
import sqlancer.tidb.ast.TiDBInOperator;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBOrderingTerm;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableAndColumnReference;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBResultMap;
import sqlancer.tidb.ast.TiDBValues;
import sqlancer.tidb.ast.TiDBValuesRow;
import sqlancer.tidb.ast.TiDBWithClause;
import sqlancer.tidb.ast.TiDBAggregate.TiDBAggregateFunction;
import sqlancer.tidb.ast.TiDBBinaryComparisonOperation.TiDBComparisonOperator;
import sqlancer.tidb.ast.TiDBBinaryLogicalOperation.TiDBBinaryLogicalOperator;
import sqlancer.tidb.ast.TiDBJoin.NaturalJoinType;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBCODDTestOracle extends CODDTestBase<TiDBGlobalState> implements TestOracle<TiDBGlobalState> {

    private final TiDBSchema s;
    private TiDBExpressionGenerator gen;
    private Reproducer<TiDBGlobalState> reproducer;

    private String tempTableName = "temp_table";

    private TiDBExpression foldedExpr;
    private TiDBExpression constantResOfFoldedExpr;


    private List<TiDBTable> tablesFromOuterContext = new ArrayList<>();
    private List<TiDBJoin> joinsInExpr = null;

    Map<String, List<TiDBConstant>> auxiliaryQueryResult = new HashMap<>();
    Map<String, List<TiDBConstant>> selectResult = new HashMap<>();

    Boolean useSubqueryAsFoldedExpr;
    Boolean useCorrelatedSubqueryAsFoldedExpr;

    TiDBCompositeDataType foldedExpressionReturnType = null;

    public TiDBCODDTestOracle(TiDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        TiDBErrors.addExpressionErrors(errors);
        TiDBErrors.addInsertErrors(errors);

        errors.add("strconv.Atoi: parsing");
        errors.add("interface conversion: expression.Expression is *expression.ScalarFunction, not *expression.Column");
        errors.add("expected integer");
        errors.add("Communications link failure");
        errors.add("No operations allowed after connection closed.");
        errors.add("ON condition doesn't support subqueries yet");
        errors.add("Can't group on"); // https://github.com/pingcap/tidb/issues/58974
        errors.addRegex(Pattern.compile("Expression\\s#\\d+\\sof\\sORDER\\sBY\\sis\\snot\\sin\\sGROUP\\sBY\\sclause\\sand\\scontains\\snonaggregated\\scolumn\\s'.*?'\\swhich\\sis\\snot\\sfunctionally\\sdependent\\son\\scolumns\\sin\\sGROUP\\sBY\\sclause;\\sthis\\sis\\sincompatible\\swith\\ssql_mode=only_full_group_by"));
    }

    @Override
    public void check() throws Exception {
        
        reproducer = null;

        joinsInExpr = null;
        tablesFromOuterContext.clear();

        useSubqueryAsFoldedExpr = useSubquery();
        useCorrelatedSubqueryAsFoldedExpr = useCorrelatedSubquery();


        TiDBSelect auxiliaryQuery = null;

        if (useSubqueryAsFoldedExpr) {
            if (useCorrelatedSubqueryAsFoldedExpr) { 
                auxiliaryQuery = genSelectWithCorrelatedSubquery();
                auxiliaryQueryString = TiDBVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult.putAll(selectResult);
            } else {
                auxiliaryQuery = genSelectExpression(null, null, null);
                auxiliaryQueryString = TiDBVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult = getQueryResult(auxiliaryQueryString, state);
            }
        } else {
            auxiliaryQuery = genSimpleSelect();
            auxiliaryQueryString = TiDBVisitor.asString(auxiliaryQuery);
            auxiliaryQueryResult.putAll(selectResult);
        }
        
        
        TiDBSelect originalQuery = null;
            
        Map<String, List<TiDBConstant>> foldedResult = new HashMap<>();
        Map<String, List<TiDBConstant>> originalResult = new HashMap<>();

        // dependent expression
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            TiDBExpressionBag specificCondition = new TiDBExpressionBag(this.foldedExpr);
            originalQuery = this.genSelectExpression(null, specificCondition, null);
            originalQueryString = TiDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            specificCondition.updateInnerExpr(this.constantResOfFoldedExpr);
            foldedQueryString = TiDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        
        // independent expression
        // empty result, put the inner query in (NOT) EXIST
        else if (auxiliaryQueryResult.size() == 0 || auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().iterator().next()).size() == 0) {
            boolean isNegated = Randomly.getBoolean() ? false : true;

            // original query
            TiDBExists existExpr = new TiDBExists(auxiliaryQuery, isNegated);
            TiDBExpressionBag specificCondition = new TiDBExpressionBag(existExpr);

            originalQuery = this.genSelectExpression(null, specificCondition, foldedExpressionReturnType);
            originalQueryString = TiDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);


            // folded query
            TiDBExpression equivalentExpr = TiDBConstant.createBooleanConstant(isNegated);
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = TiDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // Scalar Subquery: 1 column and 1 row, consider the inner query as a constant
        else if (auxiliaryQueryResult.size() == 1 && auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size() == 1 && Randomly.getBoolean()) {
            // original query
            TiDBExpressionBag specificCondition = new TiDBExpressionBag(auxiliaryQuery);
            originalQuery = this.genSelectExpression(null, specificCondition, getColumnTypeFromSelect(auxiliaryQuery).get(0));
            originalQueryString = TiDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            
            // folded query
            TiDBExpression equivalentExpr = auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).get(0);
            specificCondition.updateInnerExpr(equivalentExpr);;
            foldedQueryString = TiDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // one column
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // original query
            List<TiDBColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            TiDBColumnReference selectedColumn = new TiDBColumnReference(Randomly.fromList(columns));
            TiDBTable selectedTable = selectedColumn.getColumn().getTable();
            TiDBTableReference selectedTableRef = new TiDBTableReference(selectedTable);
            TiDBExpressionBag tableBag = new TiDBExpressionBag(selectedTableRef);

            TiDBInOperator optInOperation = new TiDBInOperator(selectedColumn, auxiliaryQuery);
            TiDBExpressionBag specificCondition = new TiDBExpressionBag(optInOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition, null);
            originalQueryString = TiDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            TiDBColumn tempColumn = new TiDBColumn("c0", getColumnTypeFromSelect(auxiliaryQuery).get(0), false, false, false);
            LinkedHashMap<TiDBColumn, List<TiDBConstant>> value = new LinkedHashMap<>();
            value.put(tempColumn, auxiliaryQueryResult.values().iterator().next());
            TiDBValues refValues = new TiDBValues(value);
            TiDBInOperator refInOperation = new TiDBInOperator(selectedColumn, refValues);
            specificCondition.updateInnerExpr(refInOperation);
            foldedQueryString = TiDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }

        // ALL
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean() && false) {
            // a bug reported related to this feature
            // original query
            List<TiDBColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            TiDBColumnReference selectedColumn = new TiDBColumnReference(Randomly.fromList(columns));
            TiDBTable selectedTable = selectedColumn.getColumn().getTable();
            TiDBTableReference selectedTableRef = new TiDBTableReference(selectedTable);
            TiDBExpressionBag tableBag = new TiDBExpressionBag(selectedTableRef);

            TiDBExpressionGenerator exprGen = new TiDBExpressionGenerator(state).setColumns(Arrays.asList(selectedColumn.getColumn()));
            TiDBExpression allOptLeft = genCondition(exprGen, null, null);
            TiDBComparisonOperator allOperator = TiDBComparisonOperator.getRandom();
            while (allOperator == TiDBComparisonOperator.NULL_SAFE_EQUALS) {
                allOperator = TiDBComparisonOperator.getRandom();
            }
            TiDBAllOperator optAllOperation = new TiDBAllOperator(allOptLeft,  auxiliaryQuery, allOperator);
            TiDBExpressionBag specificCondition = new TiDBExpressionBag(optAllOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition, null);
            originalQueryString = TiDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            TiDBColumn tempColumn = new TiDBColumn("c0", getColumnTypeFromSelect(auxiliaryQuery).get(0), false, false, false);
            LinkedHashMap<TiDBColumn, List<TiDBConstant>> value = new LinkedHashMap<>();
            value.put(tempColumn, auxiliaryQueryResult.values().iterator().next());
            TiDBValues refValues = new TiDBValues(value);
            TiDBAllOperator refAllOperation = new TiDBAllOperator(allOptLeft, refValues, allOperator);
            specificCondition.updateInnerExpr(refAllOperation);
            foldedQueryString = TiDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }

        // ANY
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // original query
            List<TiDBColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            TiDBColumnReference selectedColumn = new TiDBColumnReference(Randomly.fromList(columns));
            TiDBTable selectedTable = selectedColumn.getColumn().getTable();
            TiDBTableReference selectedTableRef = new TiDBTableReference(selectedTable);
            TiDBExpressionBag tableBag = new TiDBExpressionBag(selectedTableRef);

            TiDBExpressionGenerator exprGen = new TiDBExpressionGenerator(state).setColumns(Arrays.asList(selectedColumn.getColumn()));
            TiDBExpression anyOptLeft = genCondition(exprGen, null, null);
            TiDBComparisonOperator anyOperator = TiDBComparisonOperator.getRandom();
            while (anyOperator == TiDBComparisonOperator.NULL_SAFE_EQUALS) {
                anyOperator = TiDBComparisonOperator.getRandom();
            }
            TiDBAnyOperator optAnyOperation = new TiDBAnyOperator(anyOptLeft, auxiliaryQuery, anyOperator);
            TiDBExpressionBag specificCondition = new TiDBExpressionBag(optAnyOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition, null);
            originalQueryString = TiDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            TiDBColumn tempColumn = new TiDBColumn("c0", getColumnTypeFromSelect(auxiliaryQuery).get(0), false, false, false);
            LinkedHashMap<TiDBColumn, List<TiDBConstant>> value = new LinkedHashMap<>();
            value.put(tempColumn, auxiliaryQueryResult.values().iterator().next());
            TiDBValues refValues = new TiDBValues(value);
            TiDBAnyOperator refAnyOperation = new TiDBAnyOperator(anyOptLeft, refValues, anyOperator);
            specificCondition.updateInnerExpr(refAnyOperation);
            foldedQueryString = TiDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }

        // Row Subquery
        else {
            // original query
            TiDBTable temporaryTable =  this.genTemporaryTable(auxiliaryQuery, tempTableName);
            TiDBTableReference tempTableRef = new TiDBTableReference(temporaryTable);
                
            LinkedHashMap<TiDBColumn, List<TiDBConstant>> value = new LinkedHashMap<>();
            for (TiDBColumn c: temporaryTable.getColumns()) {
                value.put(c, auxiliaryQueryResult.get(c.getName()));
            }
            TiDBValuesRow resValues = new TiDBValuesRow(value);

            TiDBExpressionBag tempTableRefBag = new TiDBExpressionBag(tempTableRef);
            TiDBTableAndColumnReference tableAndColumnRef = new TiDBTableAndColumnReference(temporaryTable);
            TiDBWithClause withClause = null;
            if (Randomly.getBoolean() || true) {
                withClause = new TiDBWithClause(tableAndColumnRef, auxiliaryQuery);
            } else {
                // there is an error in `WITH t0(c0) AS VALUES`
                withClause = new TiDBWithClause(tableAndColumnRef, resValues); 
            }
            originalQuery = genSelectExpression(tempTableRefBag, null, null);
            originalQuery.setWithClause(withClause);
            originalQueryString = TiDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            if (Randomly.getBoolean() && false) {
                // folded query: FROM VALUES () AS table, tidb seems not support this
                originalQuery.setWithClause(null);
                TiDBAlias alias = new TiDBAlias(resValues, TiDBVisitor.asString(tableAndColumnRef));
                tempTableRefBag.updateInnerExpr(alias);
                foldedQueryString = TiDBVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (Randomly.getBoolean()) {
                // folded query: SELECT FROM () AS table
                originalQuery.setWithClause(null);
                TiDBAlias alias = null;
                if (Randomly.getBoolean() || true) {
                    alias = new TiDBAlias(auxiliaryQuery, TiDBVisitor.asString(tempTableRef));
                } else {
                    // SELECT * FROM (VALUES ROW(1)) AS t2(c0); is not supported in TiDB
                    alias = new TiDBAlias(resValues, TiDBVisitor.asString(tableAndColumnRef));
                }
                tempTableRefBag.updateInnerExpr(alias);
                foldedQueryString = TiDBVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else {
                // folded query: CREATE the table
                try {
                    this.createTemporaryTable(auxiliaryQuery, tempTableName, TiDBVisitor.asString(resValues));
                    originalQuery.setWithClause(null);
                    foldedQueryString = TiDBVisitor.asString(originalQuery);
                    foldedResult = getQueryResult(foldedQueryString, state);
                } finally {
                    dropTemporaryTable(tempTableName);
                }
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
            state.getState().getLocalState().log(auxiliaryQueryString + ";\n" +foldedQueryString + ";\n" + originalQueryString + ";");
            throw new AssertionError(auxiliaryQueryResult.toString() + " " +foldedResult.toString() + " " + originalResult.toString());
        }
    }
    
    private TiDBSelect genSelectExpression(TiDBExpressionBag tableBag, TiDBExpression specificCondition, TiDBCompositeDataType conditionType) {
        TiDBTables randomTables = s.getRandomTableNonEmptyTables();
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            for (TiDBTable t : this.tablesFromOuterContext) {
                randomTables.addTable(t);
            }
            if (this.joinsInExpr != null) {
                for (TiDBJoin j : this.joinsInExpr) {
                    TiDBTableReference lt = (TiDBTableReference) j.getLeftTable();
                    randomTables.removeTable(lt.getTable());
                    TiDBTableReference rt = (TiDBTableReference) j.getRightTable();
                    randomTables.removeTable(rt.getTable());
                }
            }
        }
        TiDBTable tempTable = null;
        TiDBTableReference tableRef = null;
        if (tableBag != null) {
            tableRef = (TiDBTableReference) tableBag.getInnerExpr();
            tempTable = tableRef.getTable();
        }
        List<TiDBColumn> columns = randomTables.getColumns();
        if (tempTable != null) {
            columns.addAll(tempTable.getColumns());
        }
        if ((!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) && this.joinsInExpr != null) {
            for (TiDBJoin j : this.joinsInExpr) {
                TiDBTable t = ((TiDBTableReference) j.getRightTable()).getTable();
                columns.addAll(t.getColumns());
                t = ((TiDBTableReference) j.getLeftTable()).getTable();
                columns.addAll(t.getColumns());
            }
        }
        gen = new TiDBExpressionGenerator(state).setColumns(columns);
        List<TiDBTable> tables = randomTables.getTables();        
        List<TiDBExpression> tableRefs = tables.stream().map(t -> new TiDBTableReference(t)).collect(Collectors.toList());

        TiDBSelect select = new TiDBSelect();

        // TiDB currently not support subquery in ON
        // List<TiDBExpression> joins = genJoinExpressions(tableRefs, state, specificCondition, conditionType);
        List<TiDBJoin> joins = new ArrayList<>();
        if ((!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr))) {
            if (this.joinsInExpr != null) {
                joins.addAll(this.joinsInExpr);
                this.joinsInExpr = null;
            }
        }
        else if (Randomly.getBoolean()) {
            joins = genJoinExpressions(tableRefs, state, specificCondition, conditionType);
        }
        if (joins.size() > 0) {
            select.setJoinClauses(joins);
        }

        if (tableBag != null) {
            TiDBTableReference outerTable = (TiDBTableReference) tableBag.getInnerExpr();
            boolean isContained = false;
            for (TiDBExpression e: tableRefs) {
                TiDBTableReference tr = (TiDBTableReference) e;
                if (tr.getTable().getName().equals(outerTable.getTable().getName())) {
                    isContained = true;
                }
            }
            if (joins.size() > 0) {
                for (TiDBJoin j : joins) {
                    TiDBTable t = ((TiDBTableReference) j.getRightTable()).getTable();
                    if (t.getName().equals(outerTable.getTable().getName())) {
                        isContained = true;
                    }
                    t = ((TiDBTableReference) j.getLeftTable()).getTable();
                    if (t.getName().equals(outerTable.getTable().getName())) {
                        isContained = true;
                    }
                }
            }
            if (!isContained) {
                tableRefs.add(tableBag);
            }
        }

        select.setFromList(tableRefs);

        select.setWhereClause(genCondition(gen, specificCondition, conditionType));
        
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByClauses(genOrderBys(gen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType));
        }

        if (Randomly.getBoolean()) {
            List<TiDBColumn> selectedColumns = Randomly.nonEmptySubset(columns);
            List<TiDBExpression> selectedAlias = new LinkedList<>();
            for (int i = 0; i < selectedColumns.size(); ++i) {
                TiDBColumnReference originalName = new TiDBColumnReference(selectedColumns.get(i));
                TiDBAlias columnAlias = new TiDBAlias(originalName, "c" + String.valueOf(i));
                selectedAlias.add(columnAlias);
            }
            select.setFetchColumns(selectedAlias);
        } else {
            TiDBColumn selectedColumn = Randomly.fromList(columns);
            TiDBColumnReference aggr = new TiDBColumnReference(selectedColumn);
            TiDBAggregateFunction windowFunction = TiDBAggregateFunction.getRandom();
            // one bug reported about this
            while(windowFunction == TiDBAggregateFunction.BIT_AND || windowFunction == TiDBAggregateFunction.BIT_OR) {
                windowFunction = TiDBAggregateFunction.getRandom();
            }
            TiDBExpression originalName = new TiDBAggregate(Arrays.asList(aggr), windowFunction);
            TiDBAlias columnAlias = new TiDBAlias(originalName, "c0");
            select.setFetchColumns(Arrays.asList(columnAlias));
            // there are many syntax error in group by with subquery, just remove it
            select.setGroupByExpressions(genGroupBys(Randomly.nonEmptySubset(columns), Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType));
            select.setGroupByExpressions(gen.generateGroupBys());

            // gen having
            // there is an error in having has not been fixed: unknown column in having clause
            if (Randomly.getBooleanWithRatherLowProbability()) {
                TiDBExpressionGenerator havingGen = new TiDBExpressionGenerator(state).setColumns(columns);
                select.setHavingClause(genCondition(havingGen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType));
            }
        }
        return select;
    }

    private TiDBExpression genCondition(TiDBExpressionGenerator conGen, TiDBExpression specificCondition, TiDBCompositeDataType conditionType) {
        TiDBExpression randomCondition = conGen.generateBooleanExpression();
        if (specificCondition != null) {
            if (conditionType == null) {
                randomCondition = new TiDBBinaryLogicalOperation(randomCondition, specificCondition, TiDBBinaryLogicalOperator.getRandom());
            } else {
                switch(conditionType.getPrimitiveDataType()) {
                    case BOOL:
                    randomCondition = new TiDBBinaryLogicalOperation(randomCondition, specificCondition, TiDBBinaryLogicalOperator.getRandom());
                        break;

                    case DECIMAL:
                    case FLOATING:
                    case INT:
                    case TEXT:
                    case CHAR:
                    case NUMERIC:
                    case BLOB:
                        randomCondition = new TiDBBinaryComparisonOperation(randomCondition, specificCondition, TiDBComparisonOperator.getRandom());
                        break;
                    default:
                        randomCondition = new TiDBBinaryLogicalOperation(randomCondition, specificCondition, TiDBBinaryLogicalOperator.getRandom());
                        break;
                }
            }
        } 
        return randomCondition;
    }

    private List<TiDBJoin> genJoinExpressions(List<TiDBExpression> tableList, TiDBGlobalState globalState, TiDBExpression specificCondition, TiDBCompositeDataType conditionType) {
        List<TiDBJoin> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBoolean()) {
            TiDBTableReference leftTable = (TiDBTableReference) tableList.remove(0);
            TiDBTableReference rightTable = (TiDBTableReference) tableList.remove(0);
            List<TiDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            TiDBExpressionGenerator joinGen = new TiDBExpressionGenerator(globalState).setColumns(columns);
            TiDBExpression randomCondition = genCondition(joinGen, specificCondition, conditionType);
            switch (TiDBJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(TiDBJoin.createInnerJoin(leftTable, rightTable, randomCondition));
                break;
            case NATURAL:
                joinExpressions.add(TiDBJoin.createNaturalJoin(leftTable, rightTable, NaturalJoinType.getRandom()));
                break;
            case STRAIGHT:
                joinExpressions.add(TiDBJoin.createStraightJoin(leftTable, rightTable, randomCondition));
                break;
            case LEFT:
                joinExpressions.add(TiDBJoin.createLeftOuterJoin(leftTable, rightTable, randomCondition));
                break;
            case RIGHT:
                joinExpressions.add(TiDBJoin.createRightOuterJoin(leftTable, rightTable, randomCondition));
                break;
            case CROSS:
                joinExpressions.add(TiDBJoin.createCrossJoin(leftTable, rightTable, randomCondition));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public List<TiDBExpression> genOrderBys(TiDBExpressionGenerator orderByGen, TiDBExpression specificCondition, TiDBCompositeDataType conditionType) {
        int exprNum = Randomly.smallNumber() + 1;
        List<TiDBExpression> newExpressions = new ArrayList<>();
        for (int i = 0; i < exprNum; ++i) {
            TiDBExpression condition = genCondition(orderByGen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType);
            if (Randomly.getBoolean()) {
                condition = new TiDBOrderingTerm(condition, Randomly.getBoolean());
            }
            newExpressions.add(condition);
        }
        return newExpressions;
    }

    private List<TiDBExpression> genGroupBys(List<TiDBColumn> columns, TiDBExpression specificCondition, TiDBCompositeDataType conditionType) {
        TiDBExpressionGenerator groupByGen = new TiDBExpressionGenerator(state).setColumns(columns);
        int exprNum = Randomly.smallNumber() + 1;
        List<TiDBExpression> newExpressions = new ArrayList<>();
        for (int i = 0; i < exprNum; ++i) {
            TiDBExpression condition = genCondition(groupByGen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType);
            newExpressions.add(condition);
        }
        return newExpressions;
    }

    private Map<String, List<TiDBConstant>> getQueryResult(String queryString, TiDBGlobalState state) throws SQLException {
        Map<String, List<TiDBConstant>> result = new LinkedHashMap<>();
        if (options.logEachSelect()) {
            logger.writeCurrent(queryString);
        }
        Statement s = null;
        try {
            s = this.con.createStatement();
            ResultSet rs = null;
            try {
                rs = s.executeQuery(queryString);
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
                            TiDBConstant constant;
                            if (rs.wasNull()) {
                                constant = TiDBConstant.createNullConstant();
                            }

                            else if (value instanceof Integer) {
                                constant = TiDBConstant.createIntConstant(BigInteger.valueOf((Integer) value));
                            } else if (value instanceof Short) {
                                constant = TiDBConstant.createIntConstant(BigInteger.valueOf((Short) value));
                            } else if (value instanceof Long) {
                                constant = TiDBConstant.createIntConstant(BigInteger.valueOf((Long) value));
                            } else if (value instanceof BigInteger) {
                                constant = TiDBConstant.createIntConstant((BigInteger) value);
                            }

                            else if (value instanceof Float) {
                                constant = TiDBConstant.createFloatConstant(Double.valueOf((Float) value));
                            } else if (value instanceof Double) {
                                constant = TiDBConstant.createFloatConstant((Double) value);
                            } else if (value instanceof BigDecimal) {
                                constant = TiDBConstant.createFloatConstant(((BigDecimal) value).doubleValue());
                            }

                            else if (value instanceof Boolean) {
                                constant = TiDBConstant.createBooleanConstant((Boolean) value);
                            }

                            else if (value instanceof String) {
                                constant = TiDBConstant.createStringConstant((String) value);
                            }

                            // else if (value instanceof byte[]) {
                            //     constant = TiDBConstant.createBitConstant(Long.parseLong(new String((byte[]) value, StandardCharsets.UTF_8), 2));
                            // }

                            else if (value == null) {
                                constant = TiDBConstant.createNullConstant();
                            } else {
                                throw new IgnoreMeException();
                            }
                            List<TiDBConstant> v = result.get(idxNameMap.get(i));
                            v.add(constant);
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
                            throw new IgnoreMeException();
                        } catch (NumberFormatException e) {
                            throw new IgnoreMeException();
                        }
                    }
                    ++resultRows;
                    if (resultRows > 100) {
                        throw new IgnoreMeException();
                    }
                }
                rs.close();
                Main.nrSuccessfulActions.addAndGet(1);
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            } else {
                state.getState().getLocalState().log(queryString);
                throw new AssertionError(e.getMessage());
            }
        } finally {
            if (s != null) {
                s.close();
            }
        }
        return result;
    }

    private TiDBTable genTemporaryTable(TiDBSelect select, String tableName) {
        List<TiDBExpression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, TiDBCompositeDataType> idxTypeMap = getColumnTypeFromSelect(select);

        List<TiDBColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            TiDBColumn column = new TiDBColumn(columnName, idxTypeMap.get(i), false, false, false);
            databaseColumns.add(column);
        }
        TiDBTable table = new TiDBTable(tableName, databaseColumns, null, false);
        for (TiDBColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private TiDBTable createTemporaryTable(TiDBSelect select, String tableName, String valuesString) throws SQLException {
        List<TiDBExpression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, TiDBCompositeDataType> idxTypeMap = getColumnTypeFromSelect(select);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + tableName + " (");
        for (int i = 0; i < columnNumber; ++i) {
            String columnTypeName = "";
            if (idxTypeMap.get(i) != null) {
                columnTypeName = idxTypeMap.get(i).getPrimitiveDataType().name();
            }
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
        Statement s = null;
        try {
            s = this.con.createStatement();
            try {
                s.execute(crateTableString);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        } finally {
            if (s != null) {
                s.close();
            }
        }

        String selectString = TiDBVisitor.asString(select);
        StringBuilder sb2 = new StringBuilder();
        if (Randomly.getBoolean()) {
            sb2.append("INSERT INTO " + tableName + " "+ selectString);
        } else {
            sb2.append("INSERT INTO " + tableName + " "+ valuesString);
        }
        
        String insertValueString = sb2.toString();
        if (options.logEachSelect()) {
            logger.writeCurrent(insertValueString);
        }
        s = null;
        try {
            s = this.con.createStatement();
            try {
                s.execute(insertValueString);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        } finally {
            s.close();
        }

        List<TiDBColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            TiDBColumn column = new TiDBColumn(columnName, idxTypeMap.get(i), false, false, false);
            databaseColumns.add(column);
        }
        TiDBTable table = new TiDBTable(tableName, databaseColumns, null, false);
        for (TiDBColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private void dropTemporaryTable(String tableName) throws SQLException {
        String dropString = "DROP TABLE " + tableName + ";";
        if (options.logEachSelect()) {
            logger.writeCurrent(dropString);
        }
        Statement s = null;
        try {
            s = this.con.createStatement();
            try {
                s.execute(dropString);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        } finally {
            s.close();
        }
    }

    private Map<Integer, TiDBCompositeDataType> getColumnTypeFromSelect(TiDBSelect select) {
        List<TiDBExpression> fetchColumns = select.getFetchColumns();
        Map<Integer, TiDBCompositeDataType> idxTypeMap = new HashMap<>();
        for (int i = 0; i < fetchColumns.size(); ++i) {
            TiDBExpression column = fetchColumns.get(i);
            TiDBCompositeDataType columnType = null;
            if (column instanceof TiDBColumnReference) {
                TiDBColumnReference c = (TiDBColumnReference) column;
                columnType = c.getColumn().getType();
            } else if (column instanceof TiDBAlias) {
                TiDBAlias a = (TiDBAlias) column;
                TiDBExpression left = a.getExpression();
                if (left instanceof TiDBColumnReference) {
                    TiDBColumnReference c = (TiDBColumnReference) left;
                    columnType = c.getColumn().getType();
                } else if (left instanceof TiDBAggregate) {
                    // TiDBAggregate aggr = (TiDBAggregate) left;
                    // List<TiDBExpression> aggrExprs =  aggr.getArgs();
                    // TiDBExpression aggrExpr = aggrExprs.get(0);
                    // if (aggrExpr instanceof TiDBColumnReference) {
                    //     TiDBColumnReference c = (TiDBColumnReference) aggrExpr;
                    //     columnType = c.getColumn().getType();
                    // } else {
                    //     throw new IgnoreMeException();
                    // }
                    columnType = new TiDBCompositeDataType(TiDBDataType.INT, 8);
                }
            } 
            if (columnType == null) {
                columnType = TiDBCompositeDataType.getRandom();
            }
            idxTypeMap.put(i, columnType);
        }

        return idxTypeMap;
    }

    private boolean compareResult(Map<String, List<TiDBConstant>> r1, Map<String, List<TiDBConstant>> r2) {
        if (r1.size() != r2.size()) {
            return false;
        }
        for (Map.Entry < String, List<TiDBConstant> > entry: r1.entrySet()) {
            String currentKey = entry.getKey();
            if (!r2.containsKey(currentKey)) {
                return false;
            } 
            List<TiDBConstant> v1= entry.getValue();
            List<TiDBConstant> v2= r2.get(currentKey);
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

    private TiDBSelect genSimpleSelect() {
        TiDBTables tables = s.getRandomTableNonEmptyTables();
        tablesFromOuterContext = tables.getTables();
        List<TiDBExpression> tableL = tables.getTables().stream().map(t -> new TiDBTableReference(t))
                .collect(Collectors.toList());
        TiDBExpressionGenerator exprGen = new TiDBExpressionGenerator(state).setColumns(tables.getColumns());
        this.foldedExpr = genCondition(exprGen, null, null);

        TiDBSelect select = new TiDBSelect();
        if (Randomly.getBoolean()) {
            List<TiDBJoin> joins = genJoinExpressions(tableL, state, null, null);
            if (joins.size() > 0) {
                select.setJoinClauses(joins);
                this.joinsInExpr = joins;
            }
        }
        select.setFromList(tableL);

        List<TiDBExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (TiDBColumn c : tables.getColumns()) {
            TiDBColumnReference cRef = new TiDBColumnReference(c);
            TiDBAlias cAlias = new TiDBAlias(cRef, "c" + String.valueOf(columnIdx));
            fetchColumns.add(cAlias);
            columnIdx++;
        }

        // add the expression as last fetch column
        TiDBAlias eAlias = new TiDBAlias(this.foldedExpr, "c" + String.valueOf(columnIdx));
        fetchColumns.add(eAlias);

        select.setFetchColumns(fetchColumns);

        originalQueryString = TiDBVisitor.asString(select);
        

        Map<String, List<TiDBConstant>> queryRes = null;
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
        List<TiDBConstant> summary = queryRes.remove("c" + String.valueOf(columnIdx));

        Boolean emptyRes = queryRes.get(queryRes.keySet().iterator().next()).size() == 0;

        Map<Integer, TiDBCompositeDataType> columnType = null;
        if (!emptyRes) {
            columnType = getColumnTypeFromSelect(select);
        }
        LinkedHashMap<TiDBColumnReference, List<TiDBConstant>> dbstate = new LinkedHashMap<>();
        // do not put the last fetch column to values
        for (int i = 0; i < fetchColumns.size() - 1; ++i) { 
            TiDBAlias cAlias = (TiDBAlias) fetchColumns.get(i);
            TiDBColumnReference cRef = (TiDBColumnReference) cAlias.getExpression();
            String columnName = cAlias.getAlias();
            dbstate.put(cRef, queryRes.get(columnName));
        }

        foldedExpressionReturnType = columnType.get(fetchColumns.size() - 1);

        this.constantResOfFoldedExpr = new TiDBResultMap(dbstate, summary, foldedExpressionReturnType);

        return select;
    }

    private TiDBSelect genSelectWithCorrelatedSubquery() {
        TiDBTables outerQueryRandomTables = s.getRandomTableNonEmptyTables();
        TiDBTables innerQueryRandomTables = s.getRandomTableNonEmptyTables();

        List<TiDBExpression> innerQueryFromTables = new ArrayList<>();
        for (TiDBTable t : innerQueryRandomTables.getTables()) {
            if (!outerQueryRandomTables.isContained(t)) {
                innerQueryFromTables.add(new TiDBTableReference(t));
            }
        }
        for (TiDBTable t : outerQueryRandomTables.getTables()) {
            if (innerQueryRandomTables.isContained(t)) {
                innerQueryRandomTables.removeTable(t);

                List<TiDBColumn> newColumns = new ArrayList<>();
                for (TiDBColumn c : t.getColumns()) {
                    TiDBColumn newColumn = new TiDBColumn(c.getName(), c.getType(), false, false, false);
                    newColumns.add(newColumn);
                }
                TiDBTable newTable = new TiDBTable(t.getName() + "a", newColumns, null, false);
                for (TiDBColumn c : newColumns) {
                    c.setTable(newTable);
                }
                innerQueryRandomTables.addTable(newTable);

                TiDBAlias alias = new TiDBAlias(new TiDBTableReference(t), newTable.getName());
                innerQueryFromTables.add(alias);
            }
        }

        List<TiDBColumn> innerQueryColumns = new ArrayList<>();
        innerQueryColumns.addAll(innerQueryRandomTables.getColumns());
        innerQueryColumns.addAll(outerQueryRandomTables.getColumns());
        gen = new TiDBExpressionGenerator(state).setColumns(innerQueryColumns);

        TiDBSelect innerQuery = new TiDBSelect();
        innerQuery.setFromList(innerQueryFromTables);

        TiDBExpression innerQueryWhereCondition = gen.generateBooleanExpression();
        innerQuery.setWhereClause(innerQueryWhereCondition);

        // use aggregate function in fetch column
        TiDBColumnReference innerQueryAggr = new TiDBColumnReference(Randomly.fromList(innerQueryRandomTables.getColumns()));
        TiDBAggregateFunction windowFunction = TiDBAggregateFunction.getRandom();
        TiDBExpression innerQueryAggrName = new TiDBAggregate(Arrays.asList(innerQueryAggr), windowFunction);
        innerQuery.setFetchColumns(Arrays.asList(innerQueryAggrName));

        this.foldedExpr = innerQuery;

        // outer query
        TiDBSelect outerQuery = new TiDBSelect();
        List<TiDBExpression> outerQueryFromTableRefs = outerQueryRandomTables.getTables().stream().map(t -> new TiDBTableReference(t)).collect(Collectors.toList());
        outerQuery.setFromList(outerQueryFromTableRefs);
        tablesFromOuterContext = outerQueryRandomTables.getTables();

        List<TiDBExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (TiDBColumn c : outerQueryRandomTables.getColumns()) {
            TiDBColumnReference cRef = new TiDBColumnReference(c);
            TiDBAlias cAlias = new TiDBAlias(cRef, "c" + String.valueOf(columnIdx));
            fetchColumns.add(cAlias);
            columnIdx++;
        }

        // add the expression as last fetch column
        TiDBAlias subqueryAlias = new TiDBAlias(innerQuery, "c" + String.valueOf(columnIdx));
        fetchColumns.add(subqueryAlias);

        outerQuery.setFetchColumns(fetchColumns);

        originalQueryString = TiDBVisitor.asString(outerQuery);
        

        Map<String, List<TiDBConstant>> queryRes = null;
        try {
            queryRes = getQueryResult(originalQueryString, state);
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(e.getMessage());
            }
        } 
        // just ignore the empty result, because of the empty table
        if (queryRes.get("c0").size() == 0) {
            throw new IgnoreMeException();
        }
        
        // save the result first
        selectResult.clear();
        selectResult.putAll(queryRes);

        // get the constant corresponding to each row from results
        List<TiDBConstant> summary = queryRes.remove("c" + String.valueOf(columnIdx));

        Boolean emptyRes = queryRes.get(queryRes.keySet().iterator().next()).size() == 0;
        Map<Integer, TiDBCompositeDataType> columnType = null;
        if (!emptyRes) {
            columnType = getColumnTypeFromSelect(outerQuery);
        }

        LinkedHashMap<TiDBColumnReference, List<TiDBConstant>> dbstate = new LinkedHashMap<>();
        // do not put the last fetch column to values
        for (int i = 0; i < fetchColumns.size() - 1; ++i) { 
            TiDBAlias cAlias = (TiDBAlias) fetchColumns.get(i);
            TiDBColumnReference cRef = (TiDBColumnReference) cAlias.getExpression();
            String columnName = cAlias.getAlias();
            dbstate.put(cRef, queryRes.get(columnName));
        }

        foldedExpressionReturnType = columnType.get(fetchColumns.size() - 1);

        this.constantResOfFoldedExpr = new TiDBResultMap(dbstate, summary, foldedExpressionReturnType);

        return outerQuery;
    }

    Boolean isEmptyTable(TiDBTable t) throws SQLException {
        String queryString = "SELECT * FROM " + TiDBVisitor.asString(new TiDBTableReference(t)) + ";";
        int resultRows = 0;
        Statement s = null;
        try {
            s = this.con.createStatement();
            ResultSet rs = null;
            try {
                rs = s.executeQuery(queryString);
                while (rs.next()) {
                    ++resultRows;
                }
                rs.close();
            } catch (SQLException e) {
                if (errors.errorIsExpected(e.getMessage())) {
                    throw new IgnoreMeException();
                } else {
                    state.getState().getLocalState().log(queryString);
                    throw new AssertionError(e.getMessage());
                }
            } finally {
                rs.close();
            }
        } finally {
            if (s != null) {
                s.close();
            }
        }
        return resultRows == 0;
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
    public Reproducer<TiDBGlobalState> getLastReproducer() {
        return reproducer;
    }
}
