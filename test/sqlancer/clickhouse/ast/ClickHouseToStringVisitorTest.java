package sqlancer.clickhouse.ast;

import org.junit.jupiter.api.Test;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.constant.ClickHouseInt8Constant;
import sqlancer.common.schema.TableIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClickHouseToStringVisitorTest {

    @Test
    void select1Test() {
        ClickHouseConstant oneConst = new ClickHouseInt8Constant(1);
        ClickHouseSelect selectOne = new ClickHouseSelect();
        selectOne.setFetchColumns(Arrays.asList(oneConst));
        String result = ClickHouseVisitor.asString(selectOne);
        String answer = "SELECT 1";
        assertEquals(answer, result);
    }

    @Test
    void select1asATest() {
        ClickHouseAliasOperation oneConstAsA = new ClickHouseAliasOperation(new ClickHouseInt8Constant(1), "a");
        ClickHouseSelect selectOne = new ClickHouseSelect();
        selectOne.setFetchColumns(Arrays.asList(oneConstAsA));
        String result = ClickHouseVisitor.asString(selectOne);
        String answer = "SELECT 1 AS `a`";
        assertEquals(answer, result);
    }

    @Test
    void selectATest() {
        List<ClickHouseSchema.ClickHouseColumn> empty_col_list = Collections.emptyList();
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseTable table = new ClickHouseSchema.ClickHouseTable("t", empty_col_list, indexes,
                false);
        ClickHouseTableReference table_ref = new ClickHouseTableReference(table, null);
        ClickHouseSchema.ClickHouseColumn a_col = new ClickHouseSchema.ClickHouseColumn("a",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        a_col.setTable(table);
        ClickHouseColumnReference a_ref = a_col.asColumnReference(null);
        ClickHouseSelect selectA = new ClickHouseSelect();
        selectA.setFetchColumns(Arrays.asList(a_ref));
        selectA.setFromClause(table_ref);
        String result = ClickHouseVisitor.asString(selectA);
        String answer = "SELECT t.a FROM t";
        assertEquals(answer, result);
    }

    @Test
    void selectAasBTest() {
        List<ClickHouseSchema.ClickHouseColumn> empty_col_list = Collections.emptyList();
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseTable table = new ClickHouseSchema.ClickHouseTable("t", empty_col_list, indexes,
                false);
        ClickHouseTableReference table_ref = new ClickHouseTableReference(table, null);
        ClickHouseSchema.ClickHouseColumn a_col = new ClickHouseSchema.ClickHouseColumn("a",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        a_col.setTable(table);
        ClickHouseColumnReference a_ref = a_col.asColumnReference(null);
        ClickHouseAliasOperation b = new ClickHouseAliasOperation(a_ref, "b");
        ClickHouseColumnReference b_ref = new ClickHouseColumnReference(b);
        ClickHouseSelect selectA = new ClickHouseSelect();
        selectA.setFetchColumns(Arrays.asList(b, b_ref));
        selectA.setFromClause(table_ref);
        String result = ClickHouseVisitor.asString(selectA);
        String answer = "SELECT t.a AS `b`, b FROM t";
        assertEquals(answer, result);
    }

    @Test
    void selectABTest() {
        List<ClickHouseSchema.ClickHouseColumn> empty_col_list = Collections.emptyList();
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseTable table = new ClickHouseSchema.ClickHouseTable("t", empty_col_list, indexes,
                false);
        ClickHouseTableReference table_ref = new ClickHouseTableReference(table, null);
        ClickHouseSchema.ClickHouseColumn a_col = new ClickHouseSchema.ClickHouseColumn("a",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        ClickHouseSchema.ClickHouseColumn b_col = new ClickHouseSchema.ClickHouseColumn("b",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        a_col.setTable(table);
        b_col.setTable(table);
        ClickHouseColumnReference a_ref = a_col.asColumnReference(null);
        ClickHouseColumnReference b_ref = b_col.asColumnReference(null);
        ClickHouseSelect selectAB = new ClickHouseSelect();
        selectAB.setFetchColumns(Arrays.asList(a_ref, b_ref));
        selectAB.setFromClause(table_ref);
        String result = ClickHouseVisitor.asString(selectAB);
        String answer = "SELECT t.a, t.b FROM t";
        assertEquals(answer, result);
    }

    @Test
    void selectWhereAGreaterBTest() {
        List<ClickHouseSchema.ClickHouseColumn> empty_col_list = Collections.emptyList();
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseTable table = new ClickHouseSchema.ClickHouseTable("t", empty_col_list, indexes,
                false);
        ClickHouseTableReference table_ref = new ClickHouseTableReference(table, null);
        ClickHouseSchema.ClickHouseColumn a_col = new ClickHouseSchema.ClickHouseColumn("a",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        ClickHouseSchema.ClickHouseColumn b_col = new ClickHouseSchema.ClickHouseColumn("b",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        a_col.setTable(table);
        b_col.setTable(table);
        ClickHouseColumnReference a_ref = a_col.asColumnReference(null);
        ClickHouseColumnReference b_ref = b_col.asColumnReference(null);
        ClickHouseSelect selectAB = new ClickHouseSelect();
        selectAB.setFetchColumns(Arrays.asList(a_ref, b_ref));
        selectAB.setFromClause(table_ref);
        selectAB.setWhereClause(new ClickHouseBinaryComparisonOperation(a_ref, b_ref,
                ClickHouseBinaryComparisonOperation.ClickHouseBinaryComparisonOperator.GREATER));
        String result = ClickHouseVisitor.asString(selectAB);
        String answer = "SELECT t.a, t.b FROM t WHERE ((t.a)>(t.b))";
        assertEquals(answer, result);
    }

    @Test
    void selectWhereAGreaterConstTest() {
        List<ClickHouseSchema.ClickHouseColumn> empty_col_list = Collections.emptyList();
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseTable table = new ClickHouseSchema.ClickHouseTable("t", empty_col_list, indexes,
                false);
        ClickHouseTableReference table_ref = new ClickHouseTableReference(table, null);
        ClickHouseSchema.ClickHouseColumn a_col = new ClickHouseSchema.ClickHouseColumn("a",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        ClickHouseSchema.ClickHouseColumn b_col = new ClickHouseSchema.ClickHouseColumn("b",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        a_col.setTable(table);
        b_col.setTable(table);
        ClickHouseColumnReference a_ref = a_col.asColumnReference(null);
        ClickHouseColumnReference b_ref = b_col.asColumnReference(null);
        ClickHouseSelect selectAB = new ClickHouseSelect();
        selectAB.setFetchColumns(Arrays.asList(a_ref, b_ref));
        selectAB.setFromClause(table_ref);
        ClickHouseConstant c_const = new ClickHouseInt8Constant(1);
        selectAB.setWhereClause(new ClickHouseBinaryComparisonOperation(a_ref, c_const,
                ClickHouseBinaryComparisonOperation.ClickHouseBinaryComparisonOperator.GREATER));
        String result = ClickHouseVisitor.asString(selectAB);
        String answer = "SELECT t.a, t.b FROM t WHERE ((t.a)>(1))";
        assertEquals(answer, result);
    }

    @Test
    void selectSumAGroupByBTest() {
        List<ClickHouseSchema.ClickHouseColumn> empty_col_list = Collections.emptyList();
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseTable table = new ClickHouseSchema.ClickHouseTable("t", empty_col_list, indexes,
                false);
        ClickHouseTableReference table_ref = new ClickHouseTableReference(table, null);
        ClickHouseSchema.ClickHouseColumn a_col = new ClickHouseSchema.ClickHouseColumn("a",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        ClickHouseSchema.ClickHouseColumn b_col = new ClickHouseSchema.ClickHouseColumn("b",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table);
        a_col.setTable(table);
        b_col.setTable(table);
        ClickHouseColumnReference a_ref = a_col.asColumnReference(null);
        ClickHouseColumnReference b_ref = b_col.asColumnReference(null);
        ClickHouseSelect selectAB = new ClickHouseSelect();
        ClickHouseAggregate sum_a = new ClickHouseAggregate(a_ref, ClickHouseAggregate.ClickHouseAggregateFunction.SUM);
        selectAB.setFetchColumns(Arrays.asList(sum_a));
        selectAB.setFromClause(table_ref);
        selectAB.setGroupByClause(Arrays.asList(b_ref));
        String result = ClickHouseVisitor.asString(selectAB);
        String answer = "SELECT SUM(t.a) FROM t GROUP BY t.b";
        assertEquals(answer, result);
    }

    @Test
    void selectCrossJoinTest() {
        List<ClickHouseSchema.ClickHouseColumn> empty_col_list = Collections.emptyList();
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseTable table1 = new ClickHouseSchema.ClickHouseTable("t1", empty_col_list, indexes,
                false);
        ClickHouseSchema.ClickHouseTable table2 = new ClickHouseSchema.ClickHouseTable("t2", empty_col_list, indexes,
                false);
        ClickHouseTableReference table1_ref = new ClickHouseTableReference(table1, null);
        ClickHouseTableReference table2_ref = new ClickHouseTableReference(table2, null);
        ClickHouseSchema.ClickHouseColumn a1_col = new ClickHouseSchema.ClickHouseColumn("a1",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table1);
        ClickHouseSchema.ClickHouseColumn b1_col = new ClickHouseSchema.ClickHouseColumn("b1",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table1);
        ClickHouseSchema.ClickHouseColumn a2_col = new ClickHouseSchema.ClickHouseColumn("a2",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table2);
        ClickHouseSchema.ClickHouseColumn b2_col = new ClickHouseSchema.ClickHouseColumn("b2",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, table2);
        a1_col.setTable(table1);
        b1_col.setTable(table1);
        a2_col.setTable(table2);
        b2_col.setTable(table2);

        ClickHouseColumnReference a1_ref = a1_col.asColumnReference(null);
        ClickHouseColumnReference b1_ref = b1_col.asColumnReference(null);
        ClickHouseColumnReference a2_ref = a2_col.asColumnReference(null);
        ClickHouseColumnReference b2_ref = b2_col.asColumnReference(null);

        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(a1_ref, a2_ref, b1_ref, b2_ref));
        select.setFromClause(table1_ref);
        ClickHouseExpression.ClickHouseJoin join = new ClickHouseExpression.ClickHouseJoin(table1_ref, table2_ref,
                ClickHouseExpression.ClickHouseJoin.JoinType.CROSS);
        select.setJoinClauses(Arrays.asList(join));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT t1.a1, t2.a2, t1.b1, t2.b2 FROM t1 JOIN t2";
        assertEquals(answer, result);
    }

    @Test
    void selectCrossJoinAliasedTest() {
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseColumn a1_col = new ClickHouseSchema.ClickHouseColumn("a1",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn b1_col = new ClickHouseSchema.ClickHouseColumn("b1",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn a2_col = new ClickHouseSchema.ClickHouseColumn("a2",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn b2_col = new ClickHouseSchema.ClickHouseColumn("b2",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseTable table1 = new ClickHouseSchema.ClickHouseTable("t1",
                Arrays.asList(a1_col, b1_col), indexes, false);
        ClickHouseSchema.ClickHouseTable table2 = new ClickHouseSchema.ClickHouseTable("t2",
                Arrays.asList(a2_col, b2_col), indexes, false);
        a1_col.setTable(table1);
        b1_col.setTable(table1);
        a2_col.setTable(table2);
        b2_col.setTable(table2);

        ClickHouseTableReference table1_ref = new ClickHouseTableReference(table1, "left");
        ClickHouseTableReference table2_ref = new ClickHouseTableReference(table2, "right");

        List<ClickHouseColumnReference> t1_col_ref = table1_ref.getColumnReferences();
        ClickHouseColumnReference a1_ref = t1_col_ref.get(0);
        ClickHouseColumnReference a2_ref = t1_col_ref.get(1);

        List<ClickHouseColumnReference> t2_col_ref = table2_ref.getColumnReferences();
        ClickHouseColumnReference b1_ref = t2_col_ref.get(0);
        ClickHouseColumnReference b2_ref = t2_col_ref.get(1);

        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(a1_ref, a2_ref, b1_ref, b2_ref));
        select.setFromClause(table1_ref);
        ClickHouseExpression.ClickHouseJoin join = new ClickHouseExpression.ClickHouseJoin(table1_ref, table2_ref,
                ClickHouseExpression.ClickHouseJoin.JoinType.CROSS);
        select.setJoinClauses(Arrays.asList(join));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT left.a1, left.b1, right.a2, right.b2 FROM t1 AS left JOIN t2 AS right";
        assertEquals(answer, result);
    }

    @Test
    void selectJoinONTest() {
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseColumn a1_col = new ClickHouseSchema.ClickHouseColumn("a1",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn b1_col = new ClickHouseSchema.ClickHouseColumn("b1",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn a2_col = new ClickHouseSchema.ClickHouseColumn("a2",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn b2_col = new ClickHouseSchema.ClickHouseColumn("b2",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseTable table1 = new ClickHouseSchema.ClickHouseTable("t1",
                Arrays.asList(a1_col, b1_col), indexes, false);
        ClickHouseSchema.ClickHouseTable table2 = new ClickHouseSchema.ClickHouseTable("t2",
                Arrays.asList(a2_col, b2_col), indexes, false);
        a1_col.setTable(table1);
        b1_col.setTable(table1);
        a2_col.setTable(table2);
        b2_col.setTable(table2);

        ClickHouseTableReference table1_ref = new ClickHouseTableReference(table1, null);
        ClickHouseTableReference table2_ref = new ClickHouseTableReference(table2, null);

        List<ClickHouseColumnReference> t1_col_ref = table1_ref.getColumnReferences();
        ClickHouseColumnReference a1_ref = t1_col_ref.get(0);
        ClickHouseColumnReference b1_ref = t1_col_ref.get(1);

        List<ClickHouseColumnReference> t2_col_ref = table2_ref.getColumnReferences();
        ClickHouseColumnReference a2_ref = t2_col_ref.get(0);
        ClickHouseColumnReference b2_ref = t2_col_ref.get(1);

        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(a1_ref, a2_ref, b1_ref, b2_ref));
        select.setFromClause(table1_ref);
        ClickHouseExpression.ClickHouseJoinOnClause on = new ClickHouseExpression.ClickHouseJoinOnClause(a1_ref,
                a2_ref);
        ClickHouseExpression.ClickHouseJoin join = new ClickHouseExpression.ClickHouseJoin(table1_ref, table2_ref,
                ClickHouseExpression.ClickHouseJoin.JoinType.INNER, on);
        select.setJoinClauses(Arrays.asList(join));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT t1.a1, t2.a2, t1.b1, t2.b2 FROM t1 INNER JOIN t2 ON ((t1.a1)=(t2.a2))";
        assertEquals(answer, result);
    }

    @Test
    void selectJoinONAliasedTest() {
        List<TableIndex> indexes = Collections.emptyList();
        ClickHouseSchema.ClickHouseColumn a1_col = new ClickHouseSchema.ClickHouseColumn("a1",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn b1_col = new ClickHouseSchema.ClickHouseColumn("b1",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn a2_col = new ClickHouseSchema.ClickHouseColumn("a2",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseColumn b2_col = new ClickHouseSchema.ClickHouseColumn("b2",
                ClickHouseSchema.ClickHouseLancerDataType.getRandom(), false, false, null);
        ClickHouseSchema.ClickHouseTable table1 = new ClickHouseSchema.ClickHouseTable("t1",
                Arrays.asList(a1_col, b1_col), indexes, false);
        ClickHouseSchema.ClickHouseTable table2 = new ClickHouseSchema.ClickHouseTable("t2",
                Arrays.asList(a2_col, b2_col), indexes, false);
        a1_col.setTable(table1);
        b1_col.setTable(table1);
        a2_col.setTable(table2);
        b2_col.setTable(table2);

        ClickHouseTableReference table1_ref = new ClickHouseTableReference(table1, "left");
        ClickHouseTableReference table2_ref = new ClickHouseTableReference(table2, "right");

        List<ClickHouseColumnReference> t1_col_ref = table1_ref.getColumnReferences();
        ClickHouseColumnReference a1_ref = t1_col_ref.get(0);
        ClickHouseColumnReference b1_ref = t1_col_ref.get(1);

        List<ClickHouseColumnReference> t2_col_ref = table2_ref.getColumnReferences();
        ClickHouseColumnReference a2_ref = t2_col_ref.get(0);
        ClickHouseColumnReference b2_ref = t2_col_ref.get(1);

        ClickHouseSelect select = new ClickHouseSelect();
        select.setFetchColumns(Arrays.asList(a1_ref, a2_ref, b1_ref, b2_ref));
        select.setFromClause(table1_ref);
        ClickHouseExpression.ClickHouseJoinOnClause on = new ClickHouseExpression.ClickHouseJoinOnClause(a1_ref,
                a2_ref);
        ClickHouseExpression.ClickHouseJoin join = new ClickHouseExpression.ClickHouseJoin(table1_ref, table2_ref,
                ClickHouseExpression.ClickHouseJoin.JoinType.INNER, on);
        select.setJoinClauses(Arrays.asList(join));
        String result = ClickHouseVisitor.asString(select);
        String answer = "SELECT left.a1, right.a2, left.b1, right.b2 FROM t1 AS left INNER JOIN t2 AS right ON ((left.a1)=(right.a2))";
        assertEquals(answer, result);
    }
}
