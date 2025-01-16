package sqlancer.cockroachdb.oracle;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.ast.CockroachDBAggregate;
import sqlancer.cockroachdb.ast.CockroachDBAlias;
import sqlancer.cockroachdb.ast.CockroachDBAllOperator;
import sqlancer.cockroachdb.ast.CockroachDBAnyOperator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryComparisonOperator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBConstant;
import sqlancer.cockroachdb.ast.CockroachDBExists;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBExpressionBag;
import sqlancer.cockroachdb.ast.CockroachDBInOperation;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBOrderingTerm;
import sqlancer.cockroachdb.ast.CockroachDBResultMap;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableAndColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBTypeof;
import sqlancer.cockroachdb.ast.CockroachDBValues;
import sqlancer.cockroachdb.ast.CockroachDBWithClasure;
import sqlancer.cockroachdb.ast.CockroachDBAggregate.CockroachDBAggregateFunction;
import sqlancer.cockroachdb.ast.CockroachDBBinaryComparisonOperator.CockroachDBComparisonOperator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation.CockroachDBBinaryLogicalOperator;
import sqlancer.cockroachdb.ast.CockroachDBConstant.CockroachDBTextConstant;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.common.oracle.CODDTestBase;
import sqlancer.common.oracle.TestOracle;


public class CockroachDBCODDTestOracle extends CODDTestBase<CockroachDBGlobalState> implements TestOracle<CockroachDBGlobalState> {

    private final CockroachDBSchema s;
    private CockroachDBExpressionGenerator gen;
    private Reproducer<CockroachDBGlobalState> reproducer;

    private String tempTableName = "temp_table";

    private CockroachDBExpression foldedExpr;
    private CockroachDBExpression constantResOfFoldedExpr;

    private List<CockroachDBTable> tablesFromOuterContext = new ArrayList<>();
    private List<CockroachDBJoin> joinsInExpr = null;

    Map<String, List<CockroachDBConstant>> auxiliaryQueryResult = new HashMap<>();
    Map<String, List<CockroachDBConstant>> selectResult = new HashMap<>();

    Boolean useSubqueryAsFoldedExpr;
    Boolean useCorrelatedSubqueryAsFoldedExpr;

    CockroachDBCompositeDataType foldedExpressionReturnType = null;

    public CockroachDBCODDTestOracle(CockroachDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("unable to vectorize execution plan");
        errors.add("mismatched physical types at index");
        errors.add("unknown signature: ");
        errors.add("null rejection requested on non-null column");
        errors.add("computed column expression cannot reference computed columns");

        // an error that hard to avoid, because use specific condition in JOIN/ORDER BY/GROUP BY/HAVING
        errors.add("ERROR: no data source matches prefix:"); 

        errors.add("expected computed column expression to have type bytes");
        errors.add("ERROR: hash-fingerprint: bytea encoded value ends with escape character");
        errors.add("ERROR: internal error: cannot overwrite");
        errors.add("volatile functions are not allowed in computed column");
        errors.add("the following columns are not indexable due to their type");
        errors.add("found in depended-on-by references, no such index in this relation");
        errors.add("context-dependent operators are not allowed in STORED COMPUTED COLUMN");
        errors.add("invalid value for kv.range_descriptor_cache.size");
        errors.add("could not parse JSON: unable to decode JSON");
        errors.add("cannot cast jsonb object to type bool");
        errors.add("ilike_escape(): invalid encoding of the first character in string");
        errors.add("ilike_escape(): invalid encoding of the last character in string");
        errors.add("ERROR: incompatible type annotation for");
        errors.add("ERROR: VALUES types float and decimal cannot be matched");
        errors.add("ERROR: VALUES types decimal and int cannot be matched");
        errors.add("ERROR: VALUES types decimal and float cannot be matched");
        errors.add("ERROR: VALUES types int and decimal cannot be matched");
        errors.add("ERROR: could not decorrelate subquery");
        errors.add("ERROR: unimplemented: apply joins with subqueries in the \"inner\" and \"outer\" contexts are not supported");
        errors.add("language: tag is not well-formed"); // a bug already reported
        errors.add("expected subquery to be lazily planned as a routine"); // a bug already reported
        errors.add("invalid encoding of the first character in string");
        errors.add("invalid encoding of the last character in string");

        errors.add("value type string doesn't match type timetz of column");
        
        // ERROR: subquery uses ungrouped column "rowid" from outer query
        errors.add("ERROR: subquery uses ungrouped column ");
    }

    @Override
    public void check() throws Exception {
        reproducer = null;

        joinsInExpr = null;
        tablesFromOuterContext.clear();

        useSubqueryAsFoldedExpr = useSubquery();
        useCorrelatedSubqueryAsFoldedExpr = useCorrelatedSubquery();

        CockroachDBSelect auxiliaryQuery = null;

        if (useSubqueryAsFoldedExpr) {
            if (useCorrelatedSubqueryAsFoldedExpr) {
                auxiliaryQuery = genSelectWithCorrelatedSubquery();
                auxiliaryQueryString = CockroachDBVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult.putAll(selectResult);
            } else {
                auxiliaryQuery = genSelectExpression(null, null, null);
                auxiliaryQueryString = CockroachDBVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult = getQueryResult(auxiliaryQueryString, state);
            }
        } else {
            auxiliaryQuery = genSimpleSelect();
            auxiliaryQueryString = CockroachDBVisitor.asString(auxiliaryQuery);
            auxiliaryQueryResult.putAll(selectResult);
        }
        

        CockroachDBSelect originalQuery = null;
            
        Map<String, List<CockroachDBConstant>> foldedResult = new HashMap<>();
        Map<String, List<CockroachDBConstant>> originalResult = new HashMap<>();

        // dependent expression
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            CockroachDBExpressionBag specificCondition = new CockroachDBExpressionBag(this.foldedExpr);
            originalQuery = this.genSelectExpression(null, specificCondition, foldedExpressionReturnType);
            originalQueryString = CockroachDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
                    
            // folded query
            specificCondition.updateInnerExpr(this.constantResOfFoldedExpr);
            foldedQueryString = CockroachDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // independent expression
        // empty result, put the inner query in (NOT) EXIST
        else if (auxiliaryQueryResult.size() == 0 || auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().iterator().next()).size() == 0) {
            boolean isNegated = Randomly.getBoolean() ? false : true;

            // original query
            CockroachDBExists existExpr = new CockroachDBExists(auxiliaryQuery, isNegated);
            CockroachDBExpressionBag specificCondition = new CockroachDBExpressionBag(existExpr);

            originalQuery = this.genSelectExpression(null, specificCondition, foldedExpressionReturnType);
            originalQueryString = CockroachDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            CockroachDBExpression equivalentExpr = CockroachDBConstant.createBooleanConstant(isNegated);
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = CockroachDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // Scalar Subquery: 1 column and 1 row, consider the inner query as a constant
        else if (auxiliaryQueryResult.size() == 1 && auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size() == 1 && Randomly.getBoolean()) {
            // original query
            CockroachDBExpressionBag specificCondition = new CockroachDBExpressionBag(auxiliaryQuery);

            originalQuery = this.genSelectExpression(null, specificCondition, getColumnTypeFromSelect(auxiliaryQuery).get(0));
            originalQueryString = CockroachDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            
            // folded query
            CockroachDBExpression equivalentExpr = auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).get(0);
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = CockroachDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // one column
        // there are bugs about `IN` and not been fixed
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // original query
            List<CockroachDBColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            CockroachDBColumnReference selectedColumn = new CockroachDBColumnReference(Randomly.fromList(columns));
            CockroachDBTable selectedTable = selectedColumn.getColumn().getTable();
            CockroachDBTableReference selectedTableRef = new CockroachDBTableReference(selectedTable);
            CockroachDBExpressionBag tableBag = new CockroachDBExpressionBag(selectedTableRef);

            CockroachDBInOperation optInOperation = new CockroachDBInOperation(selectedColumn, Arrays.asList(auxiliaryQuery));
            CockroachDBExpressionBag specificCondition = new CockroachDBExpressionBag(optInOperation);

            originalQuery = this.genSelectExpression(tableBag, specificCondition, CockroachDBDataType.BOOL.get());
            originalQueryString = CockroachDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
                
            
            // folded query
            CockroachDBColumn tempColumn = new CockroachDBColumn("c0", getColumnTypeFromSelect(auxiliaryQuery).get(0), false, false);
            LinkedHashMap<CockroachDBColumn, List<CockroachDBConstant>> value = new LinkedHashMap<>();
            value.put(tempColumn, auxiliaryQueryResult.values().iterator().next());
            CockroachDBValues refValues = new CockroachDBValues(value);
            CockroachDBInOperation refInOperation = new CockroachDBInOperation(selectedColumn, Arrays.asList(refValues));
            specificCondition.updateInnerExpr(refInOperation);
            foldedQueryString = CockroachDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // ALL
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // original query
            List<CockroachDBColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            CockroachDBColumnReference selectedColumn = new CockroachDBColumnReference(Randomly.fromList(columns));
            CockroachDBTable selectedTable = selectedColumn.getColumn().getTable();
            CockroachDBTableReference selectedTableRef = new CockroachDBTableReference(selectedTable);
            CockroachDBExpressionBag tableBag = new CockroachDBExpressionBag(selectedTableRef);

            CockroachDBExpression allOptLeft = genCondition(gen, null, null);
            CockroachDBComparisonOperator allOperator = CockroachDBComparisonOperator.getRandom();
            while (allOperator == CockroachDBComparisonOperator.IS_DISTINCT_FROM || allOperator == CockroachDBComparisonOperator.IS_NOT_DISTINCT_FROM) {
                allOperator = CockroachDBComparisonOperator.getRandom();
            }

            CockroachDBAllOperator optAllOperation = new CockroachDBAllOperator(allOptLeft, auxiliaryQuery, allOperator);
            CockroachDBExpressionBag specificCondition = new CockroachDBExpressionBag(optAllOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition, CockroachDBDataType.BOOL.get());
            originalQueryString = CockroachDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
                
            
            // folded query
            CockroachDBColumn tempColumn = new CockroachDBColumn("c0", getColumnTypeFromSelect(auxiliaryQuery).get(0), false, false);
            LinkedHashMap<CockroachDBColumn, List<CockroachDBConstant>> value = new LinkedHashMap<>();
            value.put(tempColumn, auxiliaryQueryResult.values().iterator().next());
            CockroachDBValues refValues = new CockroachDBValues(value);
            CockroachDBAllOperator refAllOperation = new CockroachDBAllOperator(allOptLeft, refValues, allOperator);
            specificCondition.updateInnerExpr(refAllOperation);
            foldedQueryString = CockroachDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }

        // ANY
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // original query
            List<CockroachDBColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            CockroachDBColumnReference selectedColumn = new CockroachDBColumnReference(Randomly.fromList(columns));
            CockroachDBTable selectedTable = selectedColumn.getColumn().getTable();
            CockroachDBTableReference selectedTableRef = new CockroachDBTableReference(selectedTable);
            CockroachDBExpressionBag tableBag = new CockroachDBExpressionBag(selectedTableRef);

            CockroachDBExpression anyOptLeft = genCondition(gen, null, null);
            CockroachDBComparisonOperator anyOperator = CockroachDBComparisonOperator.getRandom();
            while (anyOperator == CockroachDBComparisonOperator.IS_DISTINCT_FROM || anyOperator == CockroachDBComparisonOperator.IS_NOT_DISTINCT_FROM) {
                anyOperator = CockroachDBComparisonOperator.getRandom();
            }

            CockroachDBAnyOperator optAnyOperation = new CockroachDBAnyOperator(anyOptLeft, auxiliaryQuery, anyOperator);
            CockroachDBExpressionBag specificCondition = new CockroachDBExpressionBag(optAnyOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition, CockroachDBDataType.BOOL.get());
            originalQueryString = CockroachDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
                
            
            // folded query
            CockroachDBColumn tempColumn = new CockroachDBColumn("c0", getColumnTypeFromSelect(auxiliaryQuery).get(0), false, false);
            LinkedHashMap<CockroachDBColumn, List<CockroachDBConstant>> value = new LinkedHashMap<>();
            value.put(tempColumn, auxiliaryQueryResult.values().iterator().next());
            CockroachDBValues refValues = new CockroachDBValues(value);
            CockroachDBAnyOperator refAnyOperation = new CockroachDBAnyOperator(anyOptLeft, refValues, anyOperator);
            specificCondition.updateInnerExpr(refAnyOperation);
            foldedQueryString = CockroachDBVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // Row Subquery
        else {
            // original query
            CockroachDBTable temporaryTable =  this.genTemporaryTable(auxiliaryQuery, this.tempTableName);
            CockroachDBTableReference tempTableRef = new CockroachDBTableReference(temporaryTable);

            LinkedHashMap<CockroachDBColumn, List<CockroachDBConstant>> tempValue = new LinkedHashMap<>();
            for (CockroachDBColumn c : temporaryTable.getColumns()) {
                tempValue.put(c, auxiliaryQueryResult.get(c.getName()));
            }
            CockroachDBValues values = new CockroachDBValues(tempValue);

            CockroachDBExpressionBag tempTableRefBag = new CockroachDBExpressionBag(tempTableRef);
            CockroachDBTableAndColumnReference tableAndColumnRef = new CockroachDBTableAndColumnReference(temporaryTable);
            CockroachDBWithClasure withClasure = new CockroachDBWithClasure(tableAndColumnRef, auxiliaryQuery);
            originalQuery = genSelectExpression(tempTableRefBag, null, null);
            originalQuery.setWithClause(withClasure);
            originalQueryString = CockroachDBVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            if (Randomly.getBoolean()) {
                // folded query: WITH table AS VALUES ()
                withClasure.updateRight(values);
                foldedQueryString = CockroachDBVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (Randomly.getBoolean()) {
                // folded query: SELECT FROM () AS table
                originalQuery.setWithClause(null);
                CockroachDBAlias alias = null;
                if (Randomly.getBoolean()) {
                    alias = new CockroachDBAlias(auxiliaryQuery, CockroachDBVisitor.asString(tempTableRef));
                } else {
                    alias = new CockroachDBAlias(values, CockroachDBVisitor.asString(tableAndColumnRef));
                }
                tempTableRefBag.updateInnerExpr(alias);
                foldedQueryString = CockroachDBVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else {
                // folded query: CREATE the table
                try {
                    this.createTemporaryTable(auxiliaryQuery, this.tempTableName, CockroachDBVisitor.asString(values));
                    originalQuery.setWithClause(null);
                    foldedQueryString = CockroachDBVisitor.asString(originalQuery);
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
        if (!compareResult(foldedResult, originalResult)) {
            reproducer = null; // TODO
            state.getState().getLocalState().log(auxiliaryQueryString + ";\n" + foldedQueryString + ";\n" + originalQueryString + ";");
            throw new AssertionError(auxiliaryQueryResult.toString() + " " + foldedResult.toString() + " " + originalResult.toString());
        }

    }
    
    private CockroachDBSelect genSelectExpression(CockroachDBExpressionBag tempTableRefBag, CockroachDBExpression specificCondition, CockroachDBCompositeDataType conditionType) {
        CockroachDBTables randomTables = s.getRandomTableNonEmptyTables();
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            for (CockroachDBTable t : this.tablesFromOuterContext) {
                randomTables.addTable(t);
            }
            if (this.joinsInExpr != null) {
                for (CockroachDBJoin j : this.joinsInExpr) {
                    CockroachDBTableReference lt = (CockroachDBTableReference) j.getLeftTable();
                    randomTables.addTable(lt.getTable());
                    CockroachDBTableReference rt = (CockroachDBTableReference) j.getRightTable();
                    randomTables.addTable(rt.getTable());
                }
            }
        }

        List<CockroachDBColumn> columns = randomTables.getColumns();

        CockroachDBTable tempTable = null;
        CockroachDBTableReference tempTableRef = null;

        if (tempTableRefBag != null) {
            tempTableRef = (CockroachDBTableReference) tempTableRefBag.getInnerExpr();
            tempTable = tempTableRef.getTable();
        }

        if (tempTable != null) {
            columns.addAll(tempTable.getColumns());
        }

        gen = new CockroachDBExpressionGenerator(state).setColumns(columns);
        List<CockroachDBTable> tables = randomTables.getTables();
        List<CockroachDBExpression> tableRefs = tables.stream().map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
        CockroachDBSelect select = new CockroachDBSelect();
        
        if (tempTableRefBag != null) {
            Boolean isContained = false;
            for (CockroachDBExpression tre : tableRefs) {
                CockroachDBTableReference tr = (CockroachDBTableReference) tre;
                if (tr.getTable().getName().equals(tempTable.getName())) {
                    isContained = true;
                    break;
                }
            }
            if (!isContained) {
                tableRefs.add(tempTableRefBag);
            }
        }

        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            if (this.joinsInExpr != null) {
                Iterator<CockroachDBExpression> iterator = tableRefs.iterator();
                while (iterator.hasNext()) {
                    CockroachDBExpression e = iterator.next();
                    if (e instanceof CockroachDBTableReference) {
                        CockroachDBTableReference tableRef = (CockroachDBTableReference) e;
                    
                        for (CockroachDBJoin j : this.joinsInExpr) {
                            CockroachDBTableReference leftTable = (CockroachDBTableReference) j.getLeftTable();
                            CockroachDBTableReference rightTable = (CockroachDBTableReference) j.getRightTable();
                        
                            if (tableRef.getTable().equals(leftTable.getTable()) || 
                                tableRef.getTable().equals(rightTable.getTable())) {
                                iterator.remove();
                                break;
                            }
                        }
                    }
                }
            }
        }

        List<CockroachDBJoin> joinExpressions = new ArrayList<>();
        if ((!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr))) {
            if (this.joinsInExpr != null) {
                joinExpressions.addAll(this.joinsInExpr);
                this.joinsInExpr = null;
            }
        } else if (Randomly.getBoolean()) {
            joinExpressions = getJoins(tableRefs, state, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType);
        }

        select.setFromList(tableRefs);
        
        if (joinExpressions.size() > 0) {
            select.setJoinList(joinExpressions.stream().map(t -> (CockroachDBExpression) t).collect(Collectors.toList()));
        }

        select.setWhereClause(genCondition(gen, specificCondition, conditionType));
        
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByClauses(genOrderBys(specificCondition, conditionType));
        }

        if (Randomly.getBoolean()) {
            List<CockroachDBColumn> selectedColumns = Randomly.nonEmptySubset(columns);
            List<CockroachDBExpression> selectedAlias = new LinkedList<>();
            for (int i = 0; i < selectedColumns.size(); ++i) {
                CockroachDBColumnReference originalName = new CockroachDBColumnReference(selectedColumns.get(i));
                CockroachDBAlias columnAlias = new CockroachDBAlias(originalName, "c" + String.valueOf(i));
                selectedAlias.add(columnAlias);
            }
            select.setFetchColumns(selectedAlias);
        } else {
            CockroachDBColumn selectedColumn = Randomly.fromList(columns);
            CockroachDBColumnReference aggr = new CockroachDBColumnReference(selectedColumn);
            List<CockroachDBAggregateFunction> windowFunctionList = CockroachDBAggregateFunction.getAggregates(selectedColumn.getType().getPrimitiveDataType());
            // The results of these window funciton will have different precision
            if (windowFunctionList.contains(CockroachDBAggregateFunction.SQRDIFF)) {
                windowFunctionList.remove(CockroachDBAggregateFunction.SQRDIFF);
            }
            if (windowFunctionList.contains(CockroachDBAggregateFunction.VARIANCE)) {
                windowFunctionList.remove(CockroachDBAggregateFunction.VARIANCE);
            }
            // The results maybe have different order, which caused by query plan
            if (windowFunctionList.contains(CockroachDBAggregateFunction.CONCAT_AGG)) {
                windowFunctionList.remove(CockroachDBAggregateFunction.CONCAT_AGG);
            }
            // The tables from VALUES have default sorting method, but the table from alias may have
            // specific sorting method. 
            if (selectedColumn.getType().getPrimitiveDataType().equals(CockroachDBDataType.STRING)) {
                if (windowFunctionList.contains(CockroachDBAggregateFunction.MAX)) {
                    windowFunctionList.remove(CockroachDBAggregateFunction.MAX);
                }
                if (windowFunctionList.contains(CockroachDBAggregateFunction.MIN)) {
                    windowFunctionList.remove(CockroachDBAggregateFunction.MIN);
                }
            }
            CockroachDBAggregateFunction windowFunction = Randomly.fromList(windowFunctionList);
            CockroachDBExpression originalName = new CockroachDBAggregate(windowFunction, Arrays.asList(aggr));
            CockroachDBAlias columnAlias = new CockroachDBAlias(originalName, "c0");
            select.setFetchColumns(Arrays.asList(columnAlias));
            if (Randomly.getBoolean()) {
                select.setGroupByExpressions(genGroupBys(columns, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType));
            }
            // there is an error about HAVING when use subquery in HAVING
            // ERROR: subquery uses ungrouped column "rowid" from outer query
            if (Randomly.getBoolean()) {
                CockroachDBExpressionGenerator havingGen = new CockroachDBExpressionGenerator(state).setColumns(columns);
                select.setHavingClause(genCondition(havingGen, (Randomly.getBooleanWithRatherLowProbability() && (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && !useCorrelatedSubqueryAsFoldedExpr))) ? specificCondition : null, conditionType));
            }
        }
        return select;
    }

    public CockroachDBExpression genCondition(CockroachDBExpressionGenerator generator, CockroachDBExpression specificCondition, CockroachDBCompositeDataType conditionType) {
        if (specificCondition == null) {
            conditionType = null;
        }
        CockroachDBExpression randomWhereCondition = generator.generateExpression(conditionType == null ? CockroachDBDataType.BOOL.get() : conditionType);
        CockroachDBExpression whereCondition = null;
        if (specificCondition != null) {
            if (conditionType == null) {
                whereCondition = new CockroachDBBinaryLogicalOperation(randomWhereCondition, specificCondition, CockroachDBBinaryLogicalOperator.getRandom());
            } else {
                switch(conditionType.getPrimitiveDataType()) {
                    case BOOL:
                        whereCondition = new CockroachDBBinaryLogicalOperation(randomWhereCondition, specificCondition, CockroachDBBinaryLogicalOperator.getRandom());
                        break;

                    case ARRAY:
                    case BIT:
                    case BYTES:
                    case DECIMAL:
                    case FLOAT:
                    case INT:
                    case INTERVAL:
                    case JSONB:
                    case SERIAL:
                    case STRING:
                    case TIME:
                    case TIMESTAMP:
                    case TIMESTAMPTZ:
                    case TIMETZ:
                    case VARBIT:
                        whereCondition = new CockroachDBBinaryComparisonOperator(randomWhereCondition, specificCondition, CockroachDBComparisonOperator.getRandom());
                        break;
                    default:
                        throw new AssertionError(conditionType.toString());
                        // whereCondition = new CockroachDBBinaryLogicalOperation(randomWhereCondition, specificCondition, CockroachDBBinaryLogicalOperator.getRandom());
                        // break;
                }
            }
        } else {
            whereCondition = randomWhereCondition;
        }
        return whereCondition;
    }

    public List<CockroachDBJoin> getJoins(List<CockroachDBExpression> tableList,
            CockroachDBGlobalState globalState, CockroachDBExpression specificCondition, CockroachDBCompositeDataType conditionType) throws AssertionError {
        List<CockroachDBJoin> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBoolean()) {
            // CockroachDBTableReference leftTable = (CockroachDBTableReference) tableList.remove(0);
            CockroachDBExpression leftExpr = tableList.remove(0);
            CockroachDBTableReference leftTable = null;
            if (leftExpr instanceof CockroachDBTableReference) {
                leftTable = (CockroachDBTableReference) leftExpr;
            } else if (leftExpr instanceof CockroachDBExpressionBag) {
                leftTable = (CockroachDBTableReference) ((CockroachDBExpressionBag) leftExpr).getInnerExpr();
            } else {
                throw new AssertionError();
            }

            // CockroachDBTableReference rightTable = (CockroachDBTableReference) tableList.remove(0);
            CockroachDBExpression rightExpr = tableList.remove(0);
            CockroachDBTableReference rightTable = null;
            if (rightExpr instanceof CockroachDBTableReference) {
                rightTable = (CockroachDBTableReference) rightExpr;
            } else if (rightExpr instanceof CockroachDBExpressionBag) {
                rightTable = (CockroachDBTableReference) ((CockroachDBExpressionBag) rightExpr).getInnerExpr();
            } else {
                throw new AssertionError();
            }
    
            List<CockroachDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            CockroachDBExpressionGenerator joinGen = new CockroachDBExpressionGenerator(globalState)
                    .setColumns(columns);
            joinExpressions.add(CockroachDBJoin.createJoin(leftExpr, rightExpr, CockroachDBJoin.JoinType.getRandom(), joinGen.generateExpression(CockroachDBDataType.BOOL.get())));
        }
        return joinExpressions;
    }

    public List<CockroachDBExpression> genOrderBys(CockroachDBExpression specificCondition, CockroachDBCompositeDataType conditionType) {
        List<CockroachDBExpression> orderingTerms = new ArrayList<>();
        int nr = 1;
        while (Randomly.getBooleanWithSmallProbability()) {
            nr++;
        }
        for (int i = 0; i < nr; i++) {
            CockroachDBExpression expr = genCondition(gen, specificCondition, conditionType);
            if (Randomly.getBoolean()) {
                expr = new CockroachDBOrderingTerm(expr, Randomly.getBoolean());
            }
            orderingTerms.add(expr);
        }
        return orderingTerms;
    }

    private List<CockroachDBExpression> genGroupBys(List<CockroachDBColumn> columns, CockroachDBExpression specificCondition, CockroachDBCompositeDataType conditionType) {
        CockroachDBExpressionGenerator groupByGen = new CockroachDBExpressionGenerator(state).setColumns(columns);
        int exprNum = Randomly.smallNumber() + 1;
        List<CockroachDBExpression> newExpressions = new ArrayList<>();
        for (int i = 0; i < exprNum; ++i) {
            // TODO: here we need group by condition that return integer but not boolean
            CockroachDBExpression condition = genCondition(groupByGen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType);
            newExpressions.add(condition);
        }
        return newExpressions;
    }

    private Map<String, List<CockroachDBConstant>> getQueryResult(String queryString, CockroachDBGlobalState state) throws SQLException {
        Map<String, List<CockroachDBConstant>> result = new LinkedHashMap<>();
        if (options.logEachSelect()) {
            logger.writeCurrent(queryString);
        }
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
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
                            CockroachDBConstant constant;
                            if (rs.wasNull()) {
                                constant = CockroachDBConstant.createNullConstant();
                            }
                            else if (value instanceof Boolean) {
                                constant = CockroachDBConstant.createBooleanConstant((Boolean) value);
                            }
                            else if (value instanceof Integer) {
                                constant = CockroachDBConstant.createIntConstant(Long.valueOf((Integer) value));
                            } else if (value instanceof Short) {
                                constant = CockroachDBConstant.createIntConstant(Long.valueOf((Short) value));
                            } else if (value instanceof Long) {
                                constant = CockroachDBConstant.createIntConstant((Long) value);
                            }  else if (value instanceof Double) {
                                constant = CockroachDBConstant.createFloatConstant((Double) value);
                            }
                            // Usually get wrong value of bit
                            // else if (value instanceof PGobject) {
                            //     constant = (CockroachDBConstant) CockroachDBConstant.createBitConstant(Long.valueOf(((PGobject) value).getValue()));
                            // } 
                            
                            // Current there is a bug about Time and Timestamp, we just skip it now.
                            // else if (value instanceof Timestamp) {
                            //     constant = (CockroachDBConstant) CockroachDBConstant.createTimestampConstant(((Timestamp) value).getTime());
                            // } else if (value instanceof Time) {
                            //     constant = (CockroachDBConstant) CockroachDBConstant.createTimeConstant(((Time) value).getTime());
                            // } 
                            else if (value instanceof BigDecimal) {
                                constant = CockroachDBConstant.createDecimalConstant((BigDecimal) value);
                            } // BigDecimal should at last

                            // else if (value instanceof PgArray) {
                            //     throw new IgnoreMeException();
                            // }
                            else if (value instanceof String) {
                                constant = CockroachDBConstant.createStringConstant((String) value);
                            }
                            else if (value == null) {
                                constant = CockroachDBConstant.createNullConstant();
                            } else {
                                throw new IgnoreMeException();
                            }
                            List<CockroachDBConstant> v = result.get(idxNameMap.get(i));
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
                rs.close();
                Main.nrSuccessfulActions.addAndGet(1);
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

    private CockroachDBTable genTemporaryTable(CockroachDBSelect select, String tableName) {
        List<CockroachDBExpression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, CockroachDBCompositeDataType> idxTypeMap = getColumnTypeFromSelect(select);

        List<CockroachDBColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            CockroachDBColumn column = new CockroachDBColumn(columnName, idxTypeMap.get(i), false, false);
            databaseColumns.add(column);
        }
        CockroachDBTable table = new CockroachDBTable(tableName, databaseColumns, null, false);
        for (CockroachDBColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private CockroachDBTable createTemporaryTable(CockroachDBSelect select, String tableName, String valueString) throws SQLException {
        List<CockroachDBExpression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, CockroachDBCompositeDataType> idxTypeMap = getColumnTypeFromSelect(select);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + tableName + " (");
        for (int i = 0; i < columnNumber; ++i) {
            String columnTypeName = "";
            if (idxTypeMap.get(i) != null) {
                columnTypeName = idxTypeMap.get(i).toString();
            }
            sb.append("c" + String.valueOf(i) + " " + columnTypeName + ", ");
            // sb.append("c" + String.valueOf(i) + " " + idxTypeMap.get(i+1) + ", ");
            // sb.append("c" + String.valueOf(i) + ", ");
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
            } catch (SQLException e) {
                if (errors.errorIsExpected(e.getMessage())) {
                    throw new IgnoreMeException();
                } else {
                    state.getState().getLocalState().log(crateTableString);
                    throw new AssertionError(e.getMessage());
                }
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        
        String selectString = CockroachDBVisitor.asString(select);
        StringBuilder sb2 = new StringBuilder();
        if (Randomly.getBoolean()) {
            sb2.append("INSERT INTO " + tableName + " "+ selectString);
        } else {
            sb2.append("INSERT INTO " + tableName + " "+ valueString);
        }
        
        String insertValueString = sb2.toString();
        if (options.logEachSelect()) {
            logger.writeCurrent(insertValueString);
        }
        stmt = null;
        try {
            stmt = this.con.createStatement();
            try {
                stmt.execute(insertValueString);
            } catch (SQLException e) {
                dropTemporaryTable(tableName);
                if (errors.errorIsExpected(e.getMessage())) {
                    throw new IgnoreMeException();
                } else {
                    state.getState().getLocalState().log(insertValueString);
                    throw new AssertionError(e.getMessage());
                }
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        List<CockroachDBColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            CockroachDBColumn column = new CockroachDBColumn(columnName, idxTypeMap.get(i), false, false);
            databaseColumns.add(column);
        }
        CockroachDBTable table = new CockroachDBTable(tableName, databaseColumns, null, false);
        for (CockroachDBColumn c : databaseColumns) {
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
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    private Map<Integer, CockroachDBCompositeDataType> getColumnTypeFromSelect(CockroachDBSelect select) {
        CockroachDBSelect newSelect = new CockroachDBSelect(select);

        List<CockroachDBExpression> fetchColumns = newSelect.getFetchColumns();
        List<CockroachDBExpression> newFetchColumns = new ArrayList<>();

        CockroachDBSelect innerQuery = null;
        CockroachDBExpression innerQueryFetchColumn = null;
        
        for(CockroachDBExpression column : fetchColumns) {
            CockroachDBAlias columnAlias = (CockroachDBAlias) column;
            CockroachDBExpression columnExpr = columnAlias.getExpression();
            if (columnExpr instanceof CockroachDBSelect) {
                innerQuery = (CockroachDBSelect) columnExpr;
                List<CockroachDBExpression> innerQueryFetchColumns = innerQuery.getFetchColumns();
                List<CockroachDBExpression> innerQueryNewFetchColumns = new ArrayList<>();

                for (CockroachDBExpression innerQueryColumn : innerQueryFetchColumns) {
                    innerQueryFetchColumn = innerQueryColumn;
                    CockroachDBExpression typeofColumnForInnerQuery = new CockroachDBTypeof(innerQueryColumn);
                    innerQueryNewFetchColumns.add(typeofColumnForInnerQuery);
                }
                innerQuery.setFetchColumns(innerQueryNewFetchColumns);
                CockroachDBAlias subqueryTypeAlias = new CockroachDBAlias(innerQuery, "innerType");
                newFetchColumns.add(subqueryTypeAlias);
            }
            else {
                CockroachDBExpression typeofColumn = new CockroachDBTypeof(columnAlias.getExpression());
                newFetchColumns.add(typeofColumn);
            }
        }
        
        newSelect.setFetchColumns(newFetchColumns);
        Map<String, List<CockroachDBConstant>> typeResult = null;
        try {
            typeResult = getQueryResult(CockroachDBVisitor.asString(newSelect), state);
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
        Map<Integer, CockroachDBCompositeDataType> idxTypeMap = new HashMap<>();
        for (int i = 0; i < typeResult.size(); ++i) {
            String columnName = "c" + String.valueOf(i);
            CockroachDBExpression t = typeResult.get(columnName).get(0);
            String typeName = "";
            if (t instanceof CockroachDBTextConstant) {
                CockroachDBTextConstant tString = (CockroachDBTextConstant) t;
                typeName = tString.getValue();
            } else {
                typeName = "unknown";
            }
            
            CockroachDBCompositeDataType cType = new CockroachDBCompositeDataType(typeName);
            idxTypeMap.put(i, cType);
        }

        // revert innerquery
        if (innerQuery != null) {
            innerQuery.setFetchColumns(Arrays.asList(innerQueryFetchColumn));
        }

        // the type of NULL is unknown, so we need to analysis the real type
        for (Map.Entry<Integer, CockroachDBCompositeDataType> entry : idxTypeMap.entrySet()) {
            Integer index = entry.getKey();
            String typeName = entry.getValue().toString();
            if (typeName.equals("unknown")) {
                CockroachDBExpression column = fetchColumns.get(index);
                CockroachDBCompositeDataType columnType = null;
                if (column instanceof CockroachDBColumnReference) {
                    CockroachDBColumnReference c = (CockroachDBColumnReference) column;
                    columnType = c.getColumn().getType();
                } else if (column instanceof CockroachDBAlias) {
                    CockroachDBAlias a = (CockroachDBAlias) column;
                    CockroachDBExpression left = a.getExpression();
                    if (left instanceof CockroachDBColumnReference) {
                        CockroachDBColumnReference c = (CockroachDBColumnReference) left;
                        columnType = c.getColumn().getType();
                    } else if (left instanceof CockroachDBAggregate) {
                        CockroachDBAggregate aggr = (CockroachDBAggregate) left;
                        List<CockroachDBExpression> aggrExprs =  aggr.getExpr();
                        CockroachDBExpression aggrExpr = aggrExprs.get(0);
                        if (aggrExpr instanceof CockroachDBColumnReference) {
                            CockroachDBColumnReference c = (CockroachDBColumnReference) aggrExpr;
                            columnType = c.getColumn().getType();
                        } 
                    } else if (left instanceof CockroachDBSelect) {
                        CockroachDBSelect sub = (CockroachDBSelect) left;
                        CockroachDBExpression subFetchColumn = sub.getFetchColumns().get(0);
                        if (subFetchColumn instanceof CockroachDBAggregate) {
                            CockroachDBAggregate subAggr = (CockroachDBAggregate) subFetchColumn;
                            CockroachDBExpression cr = subAggr.getExpr().get(0);
                            if (cr instanceof CockroachDBColumnReference) {
                                columnType = ((CockroachDBColumnReference) cr).getColumn().getType();
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

    private boolean compareResult(Map<String, List<CockroachDBConstant>> r1, Map<String, List<CockroachDBConstant>> r2) {
        if (r1.size() != r2.size()) {
            return false;
        }
        for (Map.Entry < String, List<CockroachDBConstant> > entry: r1.entrySet()) {
            String currentKey = entry.getKey();
            if (!r2.containsKey(currentKey)) {
                return false;
            } 
            List<CockroachDBConstant> v1= entry.getValue();
            List<CockroachDBConstant> v2= r2.get(currentKey);
            if (v1.size() != v2.size()) {
                return false;
            }
            // TODO: sometimes the float value has different type, such as float and Decimal, fixed
            List<String> v1Value = new ArrayList<>(v1.stream().map(c -> c.toStringForComparison()).collect(Collectors.toList()));
            List<String> v2Value = new ArrayList<>(v2.stream().map(c -> c.toStringForComparison()).collect(Collectors.toList()));
            Collections.sort(v1Value);
            Collections.sort(v2Value);  
            if (!v1Value.equals(v2Value)) {
                state.getState().getLocalState().log(v1Value.toString() + "\n" + v2Value.toString() + "\n");
                return false;
            }
        }
        return true;
    }

    private CockroachDBSelect genSimpleSelect() {
        CockroachDBTables tables = s.getRandomTableNonEmptyTables();

        tablesFromOuterContext = tables.getTables();
        List<CockroachDBExpression> tableL = tables.getTables().stream().map(t -> new CockroachDBTableReference(t))
                .collect(Collectors.toList());
        CockroachDBExpressionGenerator exprGen = new CockroachDBExpressionGenerator(state).setColumns(tables.getColumns());
        this.foldedExpr = exprGen.generateExpression(CockroachDBDataType.BOOL.get());

        CockroachDBSelect select = new CockroachDBSelect();

        if (Randomly.getBoolean()) {
            List<CockroachDBJoin>  joins = getJoins(tableL, state, null, null);
            if (joins.size() > 0) {
                select.setJoinClauses(joins);
                this.joinsInExpr = joins;
            }
        }

        select.setFromList(tableL);

        List<CockroachDBExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (CockroachDBColumn c : tables.getColumns()) {
            CockroachDBColumnReference cRef = new CockroachDBColumnReference(c);
            CockroachDBAlias cAlias = new CockroachDBAlias(cRef, "c" + String.valueOf(columnIdx));
            fetchColumns.add(cAlias);
            columnIdx++;
        }

        // add the expression as last fetch column
        CockroachDBAlias eAlias = new CockroachDBAlias(this.foldedExpr, "c" + String.valueOf(columnIdx));
        fetchColumns.add(eAlias);

        select.setFetchColumns(fetchColumns);

        Map<String, List<CockroachDBConstant>> queryRes = null;
        try {
            queryRes = getQueryResult(CockroachDBVisitor.asString(select), state);
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
        List<CockroachDBConstant> summary = queryRes.remove("c" + String.valueOf(columnIdx));

        Boolean emptyRes = queryRes.get(queryRes.keySet().iterator().next()).size() == 0;
        Map<Integer, CockroachDBCompositeDataType> columnType = null;
        if (!emptyRes) {
            columnType = getColumnTypeFromSelect(select);
        }

        LinkedHashMap<CockroachDBColumnReference, List<CockroachDBConstant>> dbstate = new LinkedHashMap<>();
        // do not put the last fetch column to values
        for (int i = 0; i < fetchColumns.size() - 1; ++i) { 
            CockroachDBAlias cAlias = (CockroachDBAlias) fetchColumns.get(i);
            CockroachDBColumnReference cRef = (CockroachDBColumnReference) cAlias.getExpression();
            String columnName = cAlias.getAlias();
            dbstate.put(cRef, queryRes.get(columnName));
        }

        foldedExpressionReturnType = columnType.get(fetchColumns.size() - 1);

        this.constantResOfFoldedExpr = new CockroachDBResultMap(dbstate, summary, foldedExpressionReturnType);

        return select;
    }

    private CockroachDBSelect genSelectWithCorrelatedSubquery() {
        CockroachDBTables outerQueryRandomTables = s.getRandomTableNonEmptyTables();
        CockroachDBTables innerQueryRandomTables = s.getRandomTableNonEmptyTables();

        List<CockroachDBExpression> innerQueryFromTables = new ArrayList<>();
        for (CockroachDBTable t : innerQueryRandomTables.getTables()) {
            if (!outerQueryRandomTables.isContained(t)) {
                innerQueryFromTables.add(new CockroachDBTableReference(t));
            }
        }
        for (CockroachDBTable t : outerQueryRandomTables.getTables()) {
            if (innerQueryRandomTables.isContained(t)) {
                innerQueryRandomTables.removeTable(t);

                List<CockroachDBColumn> newColumns = new ArrayList<>();
                for (CockroachDBColumn c : t.getColumns()) {
                    CockroachDBColumn newColumn = new CockroachDBColumn(c.getName(), c.getType(), false, false);
                    newColumns.add(newColumn);
                }
                CockroachDBTable newTable = new CockroachDBTable(t.getName() + "a", newColumns, null, false);
                for (CockroachDBColumn c : newColumns) {
                    c.setTable(newTable);
                }
                innerQueryRandomTables.addTable(newTable);

                CockroachDBAlias alias = new CockroachDBAlias(new CockroachDBTableReference(t), newTable.getName());
                innerQueryFromTables.add(alias);
            }
        }

        List<CockroachDBColumn> innerQueryColumns = new ArrayList<>();
        innerQueryColumns.addAll(innerQueryRandomTables.getColumns());
        innerQueryColumns.addAll(outerQueryRandomTables.getColumns());
        gen = new CockroachDBExpressionGenerator(state).setColumns(innerQueryColumns);

        CockroachDBSelect innerQuery = new CockroachDBSelect();
        innerQuery.setFromList(innerQueryFromTables);

        CockroachDBExpression innerQueryWhereCondition = gen.generateExpression(CockroachDBDataType.BOOL.get());
        innerQuery.setWhereClause(innerQueryWhereCondition);

        // use aggregate function in fetch column
        CockroachDBColumnReference innerQueryAggr = new CockroachDBColumnReference(Randomly.fromList(innerQueryRandomTables.getColumns()));
        List<CockroachDBAggregateFunction> windowFunctionList = CockroachDBAggregateFunction.getAggregates(innerQueryAggr.getColumn().getType().getPrimitiveDataType());
        // The results of these window funciton will have differentprecision
        if (windowFunctionList.contains(CockroachDBAggregateFunction.SQRDIFF)) {
            windowFunctionList.remove(CockroachDBAggregateFunction.SQRDIFF);
        }
        if (windowFunctionList.contains(CockroachDBAggregateFunction.VARIANCE)) {
            windowFunctionList.remove(CockroachDBAggregateFunction.VARIANCE);
        }
        // The results maybe have different order, which caused byquery plan
        if (windowFunctionList.contains(CockroachDBAggregateFunction.CONCAT_AGG)) {
            windowFunctionList.remove(CockroachDBAggregateFunction.CONCAT_AGG);
        }
        // The tables from VALUES have default sorting method, butthe table from alias may have
        // specific sorting method. 
        if (innerQueryAggr.getColumn().getType().getPrimitiveDataType().equals(CockroachDBDataType.STRING)) {
            if (windowFunctionList.contains(CockroachDBAggregateFunction.MAX)) {
                windowFunctionList.remove(CockroachDBAggregateFunction.MAX);
            }
            if (windowFunctionList.contains(CockroachDBAggregateFunction.MIN)) {
                windowFunctionList.remove(CockroachDBAggregateFunction.MIN);
            }
        }
        CockroachDBAggregateFunction windowFunction = Randomly.fromList(windowFunctionList);
        CockroachDBExpression innerQueryAggrName = new CockroachDBAggregate(windowFunction, Arrays.asList(innerQueryAggr));
        innerQuery.setFetchColumns(Arrays.asList(innerQueryAggrName));

        this.foldedExpr = innerQuery;

        // outer query
        CockroachDBSelect outerQuery = new CockroachDBSelect();
        List<CockroachDBExpression> outerQueryFromTableRefs = outerQueryRandomTables.getTables().stream().map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
        outerQuery.setFromList(outerQueryFromTableRefs);
        tablesFromOuterContext = outerQueryRandomTables.getTables();

        List<CockroachDBExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (CockroachDBColumn c : outerQueryRandomTables.getColumns()) {
            CockroachDBColumnReference cRef = new CockroachDBColumnReference(c);
            CockroachDBAlias cAlias = new CockroachDBAlias(cRef, "c" + String.valueOf(columnIdx));
            fetchColumns.add(cAlias);
            columnIdx++;
        }

        // add the expression as last fetch column
        CockroachDBAlias subqueryAlias = new CockroachDBAlias(innerQuery, "c" + String.valueOf(columnIdx));
        fetchColumns.add(subqueryAlias);

        outerQuery.setFetchColumns(fetchColumns);

        originalQueryString = CockroachDBVisitor.asString(outerQuery);
        

        Map<String, List<CockroachDBConstant>> queryRes = null;
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
        List<CockroachDBConstant> summary = queryRes.remove("c" + String.valueOf(columnIdx));

        Boolean emptyRes = queryRes.get(queryRes.keySet().iterator().next()).size() == 0;
        Map<Integer, CockroachDBCompositeDataType> columnType = null;
        if (!emptyRes) {
            columnType = getColumnTypeFromSelect(outerQuery);
        }

        LinkedHashMap<CockroachDBColumnReference, List<CockroachDBConstant>> dbstate = new LinkedHashMap<>();
        // do not put the last fetch column to values
        for (int i = 0; i < fetchColumns.size() - 1; ++i) { 
            CockroachDBAlias cAlias = (CockroachDBAlias) fetchColumns.get(i);
            CockroachDBColumnReference cRef = (CockroachDBColumnReference) cAlias.getExpression();
            String columnName = cAlias.getAlias();
            dbstate.put(cRef, queryRes.get(columnName));
        }

        foldedExpressionReturnType = columnType.get(fetchColumns.size() - 1);

        this.constantResOfFoldedExpr = new CockroachDBResultMap(dbstate, summary, foldedExpressionReturnType);

        return outerQuery;
    }

    Boolean isEmptyTable(CockroachDBTable t) throws SQLException {
        String queryString = "SELECT * FROM " + CockroachDBVisitor.asString(new CockroachDBTableReference(t)) + ";";
        int resultRows = 0;
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
            ResultSet rs = null;
            try {
                rs = stmt.executeQuery(queryString);
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
                if (rs != null) {
                    rs.close();
                }
            }
        } finally {
            if (stmt != null) {
                stmt.close();
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
    public Reproducer<CockroachDBGlobalState> getLastReproducer() {
        return reproducer;
    }
}
