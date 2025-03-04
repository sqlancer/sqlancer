package sqlancer.mysql.oracle;

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

import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flatbuf.Bool;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.CODDTestBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLCompositeDataType;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLAlias;
import sqlancer.mysql.ast.MySQLAllOperator;
import sqlancer.mysql.ast.MySQLAnyOperator;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLExpressionBag;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLJoin.JoinType;
import sqlancer.mysql.ast.MySQLOrderByTerm;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableAndColumnReference;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLResultMap;
import sqlancer.mysql.ast.MySQLValues;
import sqlancer.mysql.ast.MySQLValuesRow;
import sqlancer.mysql.ast.MySQLWithClause;
import sqlancer.mysql.ast.MySQLAggregate.MySQLAggregateFunction;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import sqlancer.mysql.ast.MySQLOrderByTerm.MySQLOrder;
import sqlancer.mysql.MySQLVisitor;

public class MySQLCODDTestOracle extends CODDTestBase<MySQLGlobalState> implements TestOracle<MySQLGlobalState> {

    private final MySQLSchema s;
    private MySQLExpressionGenerator gen;
    private Reproducer<MySQLGlobalState> reproducer;

    private String tempTableName = "temp_table";

    private MySQLExpression foldedExpr;
    private MySQLExpression constantResOfFoldedExpr;


    private List<MySQLTable> tablesFromOuterContext = new ArrayList<>();
    private List<MySQLJoin> joinsInExpr = null;

    Map<String, List<MySQLConstant>> auxiliaryQueryResult = new HashMap<>();
    Map<String, List<MySQLConstant>> selectResult = new HashMap<>();

    Boolean useSubqueryAsFoldedExpr;
    Boolean useCorrelatedSubqueryAsFoldedExpr;

    MySQLCompositeDataType foldedExpressionReturnType = null;

    public MySQLCODDTestOracle(MySQLGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        MySQLErrors.addExpressionErrors(errors);

        // the following two errors generated as we only generate a constant in the group expression
        errors.add("Unknown column"); 
        errors.add("Can't group on");

        // this is triggered when generate a wrong group
        errors.add("this is incompatible with sql_mode=only_full_group_by"); 
    }

    @Override
    public void check() throws Exception {
        
        reproducer = null;

        joinsInExpr = null;
        tablesFromOuterContext.clear();

        useSubqueryAsFoldedExpr = useSubquery();
        useCorrelatedSubqueryAsFoldedExpr = useCorrelatedSubquery();


        MySQLSelect auxiliaryQuery = null;

        if (useSubqueryAsFoldedExpr) {
            if (useCorrelatedSubqueryAsFoldedExpr) { 
                auxiliaryQuery = genSelectWithCorrelatedSubquery();
                auxiliaryQueryString = MySQLVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult.putAll(selectResult);
            } else {
                auxiliaryQuery = genSelectExpression(null, null, null);
                auxiliaryQueryString = MySQLVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult = getQueryResult(auxiliaryQueryString, state);
            }
        } else {
            auxiliaryQuery = genSimpleSelect();
            auxiliaryQueryString = MySQLVisitor.asString(auxiliaryQuery);
            auxiliaryQueryResult.putAll(selectResult);
        }
        
        
        MySQLSelect originalQuery = null;
            
        Map<String, List<MySQLConstant>> foldedResult = new HashMap<>();
        Map<String, List<MySQLConstant>> originalResult = new HashMap<>();

        // dependent expression
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            MySQLExpressionBag specificCondition = new MySQLExpressionBag(this.foldedExpr);
            originalQuery = this.genSelectExpression(null, specificCondition, null);
            originalQueryString = MySQLVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            specificCondition.updateInnerExpr(this.constantResOfFoldedExpr);
            foldedQueryString = MySQLVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        
        // independent expression
        // empty result, put the inner query in (NOT) EXIST
        else if (auxiliaryQueryResult.size() == 0 || auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().iterator().next()).size() == 0) {
            boolean isNegated = Randomly.getBoolean() ? false : true;

            // original query
            MySQLExists existExpr = new MySQLExists(auxiliaryQuery, isNegated);
            MySQLExpressionBag specificCondition = new MySQLExpressionBag(existExpr);

            originalQuery = this.genSelectExpression(null, specificCondition, foldedExpressionReturnType);
            originalQueryString = MySQLVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);


            // folded query
            MySQLExpression equivalentExpr = MySQLConstant.createBoolean(isNegated);
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = MySQLVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // Scalar Subquery: 1 column and 1 row, consider the inner query as a constant
        else if (auxiliaryQueryResult.size() == 1 && auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size() == 1 && Randomly.getBoolean()) {
            // original query
            MySQLExpressionBag specificCondition = new MySQLExpressionBag(auxiliaryQuery);
            originalQuery = this.genSelectExpression(null, specificCondition, getColumnTypeFromSelect(auxiliaryQuery).get(0));
            originalQueryString = MySQLVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            
            // folded query
            MySQLExpression equivalentExpr = auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).get(0);
            specificCondition.updateInnerExpr(equivalentExpr);;
            foldedQueryString = MySQLVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }
        // one column
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // original query
            List<MySQLColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            MySQLColumnReference selectedColumn = new MySQLColumnReference(Randomly.fromList(columns), null);
            MySQLTable selectedTable = selectedColumn.getColumn().getTable();
            MySQLTableReference selectedTableRef = new MySQLTableReference(selectedTable);
            MySQLExpressionBag tableBag = new MySQLExpressionBag(selectedTableRef);

            MySQLInOperation optInOperation = new MySQLInOperation(selectedColumn, Arrays.asList((MySQLExpression) auxiliaryQuery), true);
            MySQLExpressionBag specificCondition = new MySQLExpressionBag(optInOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition, null);
            originalQueryString = MySQLVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            List<MySQLExpression> vs = new ArrayList<>();
            for (MySQLConstant c: auxiliaryQueryResult.values().iterator().next()) {
                vs.add(c);
            }
            MySQLInOperation refInOperation = new MySQLInOperation(selectedColumn, vs, true);
            specificCondition.updateInnerExpr(refInOperation);
            foldedQueryString = MySQLVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }

        // ALL
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // original query
            List<MySQLColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            MySQLColumnReference selectedColumn = new MySQLColumnReference(Randomly.fromList(columns), null);
            MySQLTable selectedTable = selectedColumn.getColumn().getTable();
            MySQLTableReference selectedTableRef = new MySQLTableReference(selectedTable);
            MySQLExpressionBag tableBag = new MySQLExpressionBag(selectedTableRef);

            MySQLExpressionGenerator exprGen = new MySQLExpressionGenerator(state).setColumns(Arrays.asList(selectedColumn.getColumn()));
            MySQLExpression allOptLeft = genCondition(exprGen, null, null);
            BinaryComparisonOperator allOperator = BinaryComparisonOperator.getRandom();
            while (allOperator == BinaryComparisonOperator.LIKE) {
                allOperator = BinaryComparisonOperator.getRandom();
            }
            MySQLAllOperator optAllOperation = new MySQLAllOperator(allOptLeft,  auxiliaryQuery, allOperator);
            MySQLExpressionBag specificCondition = new MySQLExpressionBag(optAllOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition, null);
            originalQueryString = MySQLVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            MySQLColumn tempColumn = new MySQLColumn("c0", getColumnTypeFromSelect(auxiliaryQuery).get(0).getPrimitiveDataType(), false, 0);
            LinkedHashMap<MySQLColumn, List<MySQLConstant>> value = new LinkedHashMap<>();
            value.put(tempColumn, auxiliaryQueryResult.values().iterator().next());
            MySQLValues refValues = new MySQLValues(value);
            MySQLAllOperator refAllOperation = new MySQLAllOperator(allOptLeft, refValues, allOperator);
            specificCondition.updateInnerExpr(refAllOperation);
            foldedQueryString = MySQLVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }

        // ANY
        else if (auxiliaryQueryResult.size() == 1 && Randomly.getBoolean()) {
            // original query
            List<MySQLColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            MySQLColumnReference selectedColumn = new MySQLColumnReference(Randomly.fromList(columns), null);
            MySQLTable selectedTable = selectedColumn.getColumn().getTable();
            MySQLTableReference selectedTableRef = new MySQLTableReference(selectedTable);
            MySQLExpressionBag tableBag = new MySQLExpressionBag(selectedTableRef);

            MySQLExpressionGenerator exprGen = new MySQLExpressionGenerator(state).setColumns(Arrays.asList(selectedColumn.getColumn()));
            MySQLExpression anyOptLeft = genCondition(exprGen, null, null);
            BinaryComparisonOperator anyOperator = BinaryComparisonOperator.getRandom();
            while (anyOperator == BinaryComparisonOperator.LIKE) {
                anyOperator = BinaryComparisonOperator.getRandom();
            }
            MySQLAnyOperator optAnyOperation = new MySQLAnyOperator(anyOptLeft, auxiliaryQuery, anyOperator);
            MySQLExpressionBag specificCondition = new MySQLExpressionBag(optAnyOperation);
            originalQuery = this.genSelectExpression(tableBag, specificCondition, null);
            originalQueryString = MySQLVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            MySQLColumn tempColumn = new MySQLColumn("c0", getColumnTypeFromSelect(auxiliaryQuery).get(0).getPrimitiveDataType(), false, 0);
            LinkedHashMap<MySQLColumn, List<MySQLConstant>> value = new LinkedHashMap<>();
            value.put(tempColumn, auxiliaryQueryResult.values().iterator().next());
            MySQLValues refValues = new MySQLValues(value);
            MySQLAnyOperator refAnyOperation = new MySQLAnyOperator(anyOptLeft, refValues, anyOperator);
            specificCondition.updateInnerExpr(refAnyOperation);
            foldedQueryString = MySQLVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        }

        // Row Subquery
        else {
            // original query
            MySQLTable temporaryTable =  this.genTemporaryTable(auxiliaryQuery, tempTableName);
            MySQLTableReference tempTableRef = new MySQLTableReference(temporaryTable);
                
            LinkedHashMap<MySQLColumn, List<MySQLConstant>> value = new LinkedHashMap<>();
            for (MySQLColumn c: temporaryTable.getColumns()) {
                value.put(c, auxiliaryQueryResult.get(c.getName()));
            }
            MySQLValuesRow resValues = new MySQLValuesRow(value);

            MySQLExpressionBag tempTableRefBag = new MySQLExpressionBag(tempTableRef);
            MySQLTableAndColumnReference tableAndColumnRef = new MySQLTableAndColumnReference(temporaryTable);
            MySQLWithClause withClasure = null;
            if (Randomly.getBoolean()) {
                withClasure = new MySQLWithClause(tableAndColumnRef, auxiliaryQuery);
            } else {
                withClasure = new MySQLWithClause(tableAndColumnRef, resValues); 
            }
            originalQuery = genSelectExpression(tempTableRefBag, null, null);
            originalQuery.setWithClause(withClasure);
            originalQueryString = MySQLVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            if (Randomly.getBoolean()) {
                // folded query: FROM VALUES () AS table, mysql seems not support this
                originalQuery.setWithClause(null);
                MySQLAlias alias = new MySQLAlias(resValues, MySQLVisitor.asString(tableAndColumnRef));
                tempTableRefBag.updateInnerExpr(alias);
                foldedQueryString = MySQLVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (Randomly.getBoolean()) {
                // folded query: SELECT FROM () AS table
                originalQuery.setWithClause(null);
                MySQLAlias alias = null;
                if (Randomly.getBoolean()) {
                    alias = new MySQLAlias(auxiliaryQuery, MySQLVisitor.asString(tempTableRef));
                } else {
                    // SELECT * FROM (VALUES ROW(1)) AS t2(c0); is not supported in MySQL
                    alias = new MySQLAlias(resValues, MySQLVisitor.asString(tableAndColumnRef));
                }
                tempTableRefBag.updateInnerExpr(alias);
                foldedQueryString = MySQLVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else {
                // folded query: CREATE the table
                try {
                    this.createTemporaryTable(auxiliaryQuery, tempTableName, MySQLVisitor.asString(resValues));
                    originalQuery.setWithClause(null);
                    foldedQueryString = MySQLVisitor.asString(originalQuery);
                    foldedResult = getQueryResult(foldedQueryString, state);
                } finally {
                    dropTemporaryTable(tempTableName);
                }
            }
        }

        if (foldedResult == null || originalResult == null) {
            throw new IgnoreMeException();
        }

        if (!compareResult(foldedResult, originalResult)) {
            reproducer = null; // TODO
            state.getState().getLocalState().log(auxiliaryQueryString + ";\n" +foldedQueryString + ";\n" + originalQueryString + ";");
            throw new AssertionError(auxiliaryQueryResult.toString() + " " +foldedResult.toString() + " " + originalResult.toString());
        }
    }
    
    private MySQLSelect genSelectExpression(MySQLExpressionBag tableBag, MySQLExpression specificCondition, MySQLCompositeDataType conditionType) {
        MySQLTables randomTables = s.getRandomTableNonEmptyTables();
        if (!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) {
            for (MySQLTable t : this.tablesFromOuterContext) {
                randomTables.addTable(t);
            }
            if (this.joinsInExpr != null) {
                for (MySQLJoin j : this.joinsInExpr) {
                    randomTables.removeTable(j.getTable());
                }
            }
        }
        MySQLTable tempTable = null;
        MySQLTableReference tableRef = null;
        if (tableBag != null) {
            tableRef = (MySQLTableReference) tableBag.getInnerExpr();
            tempTable = tableRef.getTable();
        }
        List<MySQLColumn> columns = randomTables.getColumns();
        if (tempTable != null) {
            columns.addAll(tempTable.getColumns());
        }
        if ((!useSubqueryAsFoldedExpr || (useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)) && this.joinsInExpr != null) {
            for (MySQLJoin j : this.joinsInExpr) {
                MySQLTable t = j.getTable();
                columns.addAll(t.getColumns());
            }
        }
        gen = new MySQLExpressionGenerator(state).setColumns(columns);
        List<MySQLTable> tables = randomTables.getTables();        
        List<MySQLExpression> tableRefs = tables.stream().map(t -> new MySQLTableReference(t)).collect(Collectors.toList());

        MySQLSelect select = new MySQLSelect();

        // MySQL currently not support subquery in ON
        // List<MySQLExpression> joins = genJoinExpressions(tableRefs, state, specificCondition, conditionType);
        List<MySQLJoin> joins = new ArrayList<>();
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
            MySQLTableReference outerTable = (MySQLTableReference) tableBag.getInnerExpr();
            boolean isContained = false;
            for (MySQLExpression e: tableRefs) {
                MySQLTableReference tr = (MySQLTableReference) e;
                if (tr.getTable().getName().equals(outerTable.getTable().getName())) {
                    isContained = true;
                }
            }
            if (joins.size() > 0) {
                for (MySQLJoin j : joins) {
                    MySQLTable t = j.getTable();
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
            List<MySQLColumn> selectedColumns = Randomly.nonEmptySubset(columns);
            List<MySQLExpression> selectedAlias = new LinkedList<>();
            for (int i = 0; i < selectedColumns.size(); ++i) {
                MySQLColumnReference originalName = new MySQLColumnReference(selectedColumns.get(i), null);
                MySQLAlias columnAlias = new MySQLAlias(originalName, "c" + String.valueOf(i));
                selectedAlias.add(columnAlias);
            }
            select.setFetchColumns(selectedAlias);
        } else {
            MySQLColumn selectedColumn = Randomly.fromList(columns);
            MySQLColumnReference aggr = new MySQLColumnReference(selectedColumn, null);
            MySQLAggregateFunction windowFunction = MySQLAggregateFunction.getRandom();
            // one bug reported about this
            while(windowFunction == MySQLAggregateFunction.BIT_AND || windowFunction == MySQLAggregateFunction.BIT_OR) {
                windowFunction = MySQLAggregateFunction.getRandom();
            }
            MySQLExpression originalName = new MySQLAggregate(Arrays.asList(aggr), windowFunction);
            MySQLAlias columnAlias = new MySQLAlias(originalName, "c0");
            select.setFetchColumns(Arrays.asList(columnAlias));
            // there are many syntax error in group by with subquery, just remove it
            select.setGroupByExpressions(genGroupBys(Randomly.nonEmptySubset(columns), Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType));
            select.setGroupByExpressions(gen.generateGroupBys());

            // gen having
            // there is an error in having has not been fixed: unknown column in having clause
            if (Randomly.getBooleanWithRatherLowProbability()) {
                MySQLExpressionGenerator havingGen = new MySQLExpressionGenerator(state).setColumns(columns);
                select.setHavingClause(genCondition(havingGen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType));
            }
        }
        return select;
    }

    private MySQLExpression genCondition(MySQLExpressionGenerator conGen, MySQLExpression specificCondition, MySQLCompositeDataType conditionType) {
        MySQLExpression randomCondition = conGen.generateBooleanExpression();
        if (specificCondition != null) {
            if (conditionType == null) {
                randomCondition = new MySQLBinaryLogicalOperation(randomCondition, specificCondition, MySQLBinaryLogicalOperator.getRandom());
            } else {
                switch(conditionType.getPrimitiveDataType()) {
                    // case BOOL:
                    // randomCondition = new MySQLBinaryLogicalOperation(randomCondition, specificCondition, MySQLBinaryLogicalOperator.getRandom());
                    //     break;

                    case DECIMAL:
                    case INT:
                    case VARCHAR:
                    case FLOAT:
                    case DOUBLE:
                        randomCondition = new MySQLBinaryComparisonOperation(randomCondition, specificCondition, BinaryComparisonOperator.getRandom());
                        break;
                    default:
                        randomCondition = new MySQLBinaryLogicalOperation(randomCondition, specificCondition, MySQLBinaryLogicalOperator.getRandom());
                        break;
                }
            }
        } 
        return randomCondition;
    }

    private List<MySQLJoin> genJoinExpressions(List<MySQLExpression> tableList, MySQLGlobalState globalState, MySQLExpression specificCondition, MySQLCompositeDataType conditionType) {
        List<MySQLJoin> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBoolean()) {
            MySQLTableReference rightTable = (MySQLTableReference) tableList.remove(0);
            List<MySQLColumn> columns = new ArrayList<>(rightTable.getTable().getColumns());

            MySQLExpressionGenerator joinGen = new MySQLExpressionGenerator(globalState).setColumns(columns);
            MySQLExpression randomCondition = genCondition(joinGen, specificCondition, conditionType);
            JoinType selectedOption = Randomly.fromList(Arrays.asList(JoinType.values()));
            joinExpressions.add(new MySQLJoin(rightTable.getTable(), randomCondition, selectedOption));
        }
        return joinExpressions;
    }

    public List<MySQLExpression> genOrderBys(MySQLExpressionGenerator orderByGen, MySQLExpression specificCondition, MySQLCompositeDataType conditionType) {
        int exprNum = Randomly.smallNumber() + 1;
        List<MySQLExpression> newExpressions = new ArrayList<>();
        for (int i = 0; i < exprNum; ++i) {
            MySQLExpression condition = genCondition(orderByGen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType);
            if (Randomly.getBoolean()) {
                condition = new MySQLOrderByTerm(condition, MySQLOrder.getRandomOrder());
            }
            newExpressions.add(condition);
        }
        return newExpressions;
    }

    private List<MySQLExpression> genGroupBys(List<MySQLColumn> columns, MySQLExpression specificCondition, MySQLCompositeDataType conditionType) {
        MySQLExpressionGenerator groupByGen = new MySQLExpressionGenerator(state).setColumns(columns);
        int exprNum = Randomly.smallNumber() + 1;
        List<MySQLExpression> newExpressions = new ArrayList<>();
        for (int i = 0; i < exprNum; ++i) {
            MySQLExpression condition = genCondition(groupByGen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, conditionType);
            newExpressions.add(condition);
        }
        return newExpressions;
    }

    private Map<String, List<MySQLConstant>> getQueryResult(String queryString, MySQLGlobalState state) throws SQLException {
        Map<String, List<MySQLConstant>> result = new LinkedHashMap<>();
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
                            MySQLConstant constant;
                            if (rs.wasNull()) {
                                constant = MySQLConstant.createNullConstant();
                            }

                            else if (value instanceof Integer) {
                                constant = MySQLConstant.createIntConstant(Long.valueOf((Integer) value));
                            } else if (value instanceof Short) {
                                constant = MySQLConstant.createIntConstant(Long.valueOf((Short) value));
                            } else if (value instanceof Long) {
                                constant = MySQLConstant.createIntConstant((Long) value);
                            } else if (value instanceof java.math.BigInteger) {
                                constant = MySQLConstant.createIntConstant(((java.math.BigInteger) value).longValue());
                            } else if (value instanceof java.math.BigDecimal) {
                                constant = MySQLConstant.createIntConstant(((java.math.BigDecimal) value).longValue());
                            }

                            else if (value instanceof Float) {
                                constant = MySQLConstant.createDoubleConstant(Double.valueOf((Float) value));
                            } else if (value instanceof Double) {
                                constant = MySQLConstant.createDoubleConstant((Double) value);
                            }

                            else if (value instanceof String) {
                                constant = MySQLConstant.createStringConstant((String) value);
                            }

                            else if (value instanceof Boolean) {
                                constant = MySQLConstant.createBoolean((Boolean) value);
                            }

                            else if (value == null) {
                                constant = MySQLConstant.createNullConstant();
                            } else {
                                throw new AssertionError(value.getClass().getName());
                            }
                            List<MySQLConstant> v = result.get(idxNameMap.get(i));
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

    private MySQLTable genTemporaryTable(MySQLSelect select, String tableName) throws SQLException {
        List<MySQLExpression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, MySQLCompositeDataType> idxTypeMap = getColumnTypeFromSelect(select);

        List<MySQLColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            MySQLColumn column = new MySQLColumn(columnName, idxTypeMap.get(i).getPrimitiveDataType(), false, 0);
            databaseColumns.add(column);
        }
        MySQLTable table = new MySQLTable(tableName, databaseColumns, null, null);
        for (MySQLColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private MySQLTable createTemporaryTable(MySQLSelect select, String tableName, String valuesString) throws SQLException {
        List<MySQLExpression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, MySQLCompositeDataType> idxTypeMap = getColumnTypeFromSelect(select);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + tableName + " (");
        for (int i = 0; i < columnNumber; ++i) {
            String columnTypeName = "";
            if (idxTypeMap.get(i) != null) {
                columnTypeName = idxTypeMap.get(i).toString();
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
        Statement s = null;
        try {
            s = this.con.createStatement();
            s.execute(crateTableString);
        } finally {
            if (s != null) {
                s.close();
            }
        }

        String selectString = MySQLVisitor.asString(select);
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
            s.execute(insertValueString);
        } finally {
            s.close();
        }

        List<MySQLColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + String.valueOf(i);
            MySQLColumn column = new MySQLColumn(columnName, idxTypeMap.get(i).getPrimitiveDataType(), false, 0);
            databaseColumns.add(column);
        }
        MySQLTable table = new MySQLTable(tableName, databaseColumns, null, null);
        for (MySQLColumn c : databaseColumns) {
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
            s.execute(dropString);
        } finally {
            s.close();
        }
    }

    private Map<Integer, MySQLCompositeDataType> getColumnTypeFromSelect(MySQLSelect select) throws SQLException {
        Map<Integer, MySQLCompositeDataType> idxTypeMap = new HashMap<>();
        String tempTableName = "temp0";
        String tempTableCreate = "CREATE TABLE " + tempTableName + " AS " + MySQLVisitor.asString(select);
        String tableDescribe = "DESCRIBE " + tempTableName;
        if (options.logEachSelect()) {
            logger.writeCurrent(tempTableCreate);
        }
        Statement s = null;
        try {
            s = this.con.createStatement();
            s.execute(tempTableCreate);
        } catch(Exception e) {
            if (e.getMessage().contains("Data truncation:")) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(e.getMessage());
            }
        } finally {
            s.close();
        }
        Map<String, List<MySQLConstant>> typeResult = null;
        try {
            typeResult = getQueryResult(tableDescribe, state);
        } finally {
            dropTemporaryTable(tempTableName);
        }
        if (typeResult == null) {
            throw new AssertionError("can not get the return type of query");
        }
        // the first column, c0, of typeResult is the name of column,
        // the second column, c1, of typeResult is the type of column.
        List<MySQLConstant> types = typeResult.get("c1");
        for (int i = 0; i < types.size(); ++i) {
            String typeName = "";
            if (types.get(i) instanceof MySQLConstant) {
                MySQLConstant tString = (MySQLConstant) types.get(i);
                typeName = tString.getTextRepresentation();
            } else {
                throw new AssertionError(types.get(i).getClass().toString());
            }
            
            MySQLCompositeDataType cType = new MySQLCompositeDataType(typeName);
            idxTypeMap.put(i, cType);
        }

        return idxTypeMap;
    }

    private boolean compareResult(Map<String, List<MySQLConstant>> r1, Map<String, List<MySQLConstant>> r2) {
        if (r1.size() != r2.size()) {
            return false;
        }
        for (Map.Entry < String, List<MySQLConstant> > entry: r1.entrySet()) {
            String currentKey = entry.getKey();
            if (!r2.containsKey(currentKey)) {
                return false;
            } 
            List<MySQLConstant> v1= entry.getValue();
            List<MySQLConstant> v2= r2.get(currentKey);
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

    private MySQLSelect genSimpleSelect() throws SQLException {
        MySQLTables tables = s.getRandomTableNonEmptyTables();
        tablesFromOuterContext = tables.getTables();
        List<MySQLExpression> tableL = tables.getTables().stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        MySQLExpressionGenerator exprGen = new MySQLExpressionGenerator(state).setColumns(tables.getColumns());
        this.foldedExpr = genCondition(exprGen, null, null);

        MySQLSelect select = new MySQLSelect();
        if (Randomly.getBoolean()) {
            List<MySQLJoin> joins = genJoinExpressions(tableL, state, null, null);
            if (joins.size() > 0) {
                select.setJoinClauses(joins);
                this.joinsInExpr = joins;
            }
        }
        select.setFromList(tableL);

        List<MySQLExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (MySQLColumn c : tables.getColumns()) {
            MySQLColumnReference cRef = new MySQLColumnReference(c, null);
            MySQLAlias cAlias = new MySQLAlias(cRef, "c" + String.valueOf(columnIdx));
            fetchColumns.add(cAlias);
            columnIdx++;
        }

        // add the expression as last fetch column
        MySQLAlias eAlias = new MySQLAlias(this.foldedExpr, "c" + String.valueOf(columnIdx));
        fetchColumns.add(eAlias);

        select.setFetchColumns(fetchColumns);

        originalQueryString = MySQLVisitor.asString(select);
        

        Map<String, List<MySQLConstant>> queryRes = null;
        queryRes = getQueryResult(originalQueryString, state);
        if (queryRes.get("c0").size() == 0) {
            throw new IgnoreMeException();
        }
        
        // save the result first
        selectResult.clear();
        selectResult.putAll(queryRes);

        // get the constant corresponding to each row from results
        List<MySQLConstant> summary = queryRes.remove("c" + String.valueOf(columnIdx));

        Boolean emptyRes = queryRes.get(queryRes.keySet().iterator().next()).size() == 0;

        Map<Integer, MySQLCompositeDataType> columnType = null;
        if (!emptyRes) {
            columnType = getColumnTypeFromSelect(select);
        }
        HashMap<MySQLColumnReference, MySQLCompositeDataType> ct = new HashMap<>();
        LinkedHashMap<MySQLColumnReference, List<MySQLConstant>> dbstate = new LinkedHashMap<>();
        // do not put the last fetch column to values
        for (int i = 0; i < fetchColumns.size() - 1; ++i) { 
            MySQLAlias cAlias = (MySQLAlias) fetchColumns.get(i);
            MySQLColumnReference cRef = (MySQLColumnReference) cAlias.getExpression();
            String columnName = cAlias.getAlias();
            dbstate.put(cRef, queryRes.get(columnName));
            ct.put(cRef, columnType.get(i));
        }

        foldedExpressionReturnType = columnType.get(fetchColumns.size() - 1);

        this.constantResOfFoldedExpr = new MySQLResultMap(dbstate, ct, summary, foldedExpressionReturnType);

        return select;
    }

    private MySQLSelect genSelectWithCorrelatedSubquery() throws SQLException {
        MySQLTables outerQueryRandomTables = s.getRandomTableNonEmptyTables();
        MySQLTables innerQueryRandomTables = s.getRandomTableNonEmptyTables();

        List<MySQLExpression> innerQueryFromTables = new ArrayList<>();
        for (MySQLTable t : innerQueryRandomTables.getTables()) {
            if (!outerQueryRandomTables.isContained(t)) {
                innerQueryFromTables.add(new MySQLTableReference(t));
            }
        }
        for (MySQLTable t : outerQueryRandomTables.getTables()) {
            if (innerQueryRandomTables.isContained(t)) {
                innerQueryRandomTables.removeTable(t);

                List<MySQLColumn> newColumns = new ArrayList<>();
                for (MySQLColumn c : t.getColumns()) {
                    MySQLColumn newColumn = new MySQLColumn(c.getName(), c.getType(), false, 0);
                    newColumns.add(newColumn);
                }
                MySQLTable newTable = new MySQLTable(t.getName() + "a", newColumns, null, null);
                for (MySQLColumn c : newColumns) {
                    c.setTable(newTable);
                }
                innerQueryRandomTables.addTable(newTable);

                MySQLAlias alias = new MySQLAlias(new MySQLTableReference(t), newTable.getName());
                innerQueryFromTables.add(alias);
            }
        }

        List<MySQLColumn> innerQueryColumns = new ArrayList<>();
        innerQueryColumns.addAll(innerQueryRandomTables.getColumns());
        innerQueryColumns.addAll(outerQueryRandomTables.getColumns());
        gen = new MySQLExpressionGenerator(state).setColumns(innerQueryColumns);

        MySQLSelect innerQuery = new MySQLSelect();
        innerQuery.setFromList(innerQueryFromTables);

        MySQLExpression innerQueryWhereCondition = gen.generateBooleanExpression();
        innerQuery.setWhereClause(innerQueryWhereCondition);

        // use aggregate function in fetch column
        MySQLColumnReference innerQueryAggr = new MySQLColumnReference(Randomly.fromList(innerQueryRandomTables.getColumns()), null);
        MySQLAggregateFunction windowFunction = MySQLAggregateFunction.getRandom();
        MySQLExpression innerQueryAggrName = new MySQLAggregate(Arrays.asList(innerQueryAggr), windowFunction);
        innerQuery.setFetchColumns(Arrays.asList(innerQueryAggrName));

        this.foldedExpr = innerQuery;

        // outer query
        MySQLSelect outerQuery = new MySQLSelect();
        List<MySQLExpression> outerQueryFromTableRefs = outerQueryRandomTables.getTables().stream().map(t -> new MySQLTableReference(t)).collect(Collectors.toList());
        outerQuery.setFromList(outerQueryFromTableRefs);
        tablesFromOuterContext = outerQueryRandomTables.getTables();

        List<MySQLExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (MySQLColumn c : outerQueryRandomTables.getColumns()) {
            MySQLColumnReference cRef = new MySQLColumnReference(c, null);
            MySQLAlias cAlias = new MySQLAlias(cRef, "c" + String.valueOf(columnIdx));
            fetchColumns.add(cAlias);
            columnIdx++;
        }

        // add the expression as last fetch column
        MySQLAlias subqueryAlias = new MySQLAlias(innerQuery, "c" + String.valueOf(columnIdx));
        fetchColumns.add(subqueryAlias);

        outerQuery.setFetchColumns(fetchColumns);

        originalQueryString = MySQLVisitor.asString(outerQuery);
        

        Map<String, List<MySQLConstant>> queryRes = null;
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
        List<MySQLConstant> summary = queryRes.remove("c" + String.valueOf(columnIdx));

        Boolean emptyRes = queryRes.get(queryRes.keySet().iterator().next()).size() == 0;
        Map<Integer, MySQLCompositeDataType> columnType = null;
        if (!emptyRes) {
            columnType = getColumnTypeFromSelect(outerQuery);
        }

        LinkedHashMap<MySQLColumnReference, List<MySQLConstant>> dbstate = new LinkedHashMap<>();
        HashMap<MySQLColumnReference, MySQLCompositeDataType> ct = new HashMap<>();
        // do not put the last fetch column to values
        for (int i = 0; i < fetchColumns.size() - 1; ++i) { 
            MySQLAlias cAlias = (MySQLAlias) fetchColumns.get(i);
            MySQLColumnReference cRef = (MySQLColumnReference) cAlias.getExpression();
            String columnName = cAlias.getAlias();
            dbstate.put(cRef, queryRes.get(columnName));
            ct.put(cRef, columnType.get(i));
        }

        foldedExpressionReturnType = columnType.get(fetchColumns.size() - 1);

        this.constantResOfFoldedExpr = new MySQLResultMap(dbstate, ct, summary, foldedExpressionReturnType);

        return outerQuery;
    }

    Boolean isEmptyTable(MySQLTable t) throws SQLException {
        String queryString = "SELECT * FROM " + MySQLVisitor.asString(new MySQLTableReference(t)) + ";";
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
    public Reproducer<MySQLGlobalState> getLastReproducer() {
        return reproducer;
    }
}
