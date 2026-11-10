package sqlancer.voltdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.voltdb.VoltDBErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.VoltDBToStringVisitor;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator.Kind;

/**
 * A materialized view of one table: its rows grouped by zero to two columns, with COUNT(*) and up to three more of
 * COUNT, SUM, MIN and MAX, and sometimes a WHERE clause (only when --avoid-known-bugs is false). VoltDB keeps it up to
 * date as the table changes. The view's defining query is recorded in the global state, for the VIEWS oracle to compare
 * the view with.
 */
public final class VoltDBViewGenerator {

    private VoltDBViewGenerator() {
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState) {
        VoltDBTable table = globalState.getSchema().getRandomTable();
        boolean avoidKnownBugs = globalState.getDbmsSpecificOptions().avoidKnownBugs;
        // VoltDB does not GROUP BY a BOOLEAN expression
        List<VoltDBColumn> groupable = table.getColumns().stream()
                .filter(c -> Kind.fromType(c.getType().getType()) != Kind.BOOLEAN).collect(Collectors.toList());
        List<VoltDBColumn> grouping = Randomly.subset(groupable);
        grouping = grouping.subList(0, Math.min(2, grouping.size()));
        List<String> outputs = grouping.stream().map(VoltDBColumn::getName).collect(Collectors.toList());
        outputs.add("COUNT(*)");
        for (int i = Randomly.smallNumber(); i >= 0 && outputs.size() < grouping.size() + 4; i--) {
            VoltDBColumn c = Randomly.fromList(table.getColumns());
            Kind kind = Kind.fromType(c.getType().getType());
            List<String> functions = new ArrayList<>(List.of("COUNT"));
            // ENG-30584: deleting a group's last non-NULL value leaves 0 in the view's SUM instead of NULL, so unless
            // --avoid-known-bugs is false, a view sums NOT NULL columns only
            // ENG-30588: an UPDATE of an indexed column whose view SUM overflows fails, but leaves the row out of the
            // index, and the next UPDATE of the row crashes the node, so unless --avoid-known-bugs is false, a view
            // sums TINYINT, SMALLINT and INTEGER columns only, whose sums can't overflow
            if (kind == Kind.NUMERIC && !(avoidKnownBugs && (c.isNullable() || !sumCannotOverflow(c)))) {
                functions.add("SUM");
            }
            if (kind != Kind.BOOLEAN) {
                // ENG-30493: VoltDB orders BOOLEAN values TRUE < FALSE, so no MIN or MAX of them
                functions.add("MIN");
                functions.add("MAX");
            }
            String aggregate = Randomly.fromList(functions) + "(" + c.getName() + ")";
            // ENG-30582: VoltDB's view rewriter fails ("Duplicate key") on a matching query with the same aggregate
            // twice, and the view's own query matches it
            if (!(avoidKnownBugs && outputs.contains(aggregate))) {
                outputs.add(aggregate);
            }
        }
        StringBuilder query = new StringBuilder("SELECT ").append(String.join(", ", outputs)).append(" FROM ")
                .append(table.getName());
        // ENG-30583: a view's WHERE clause can block later DDL: a FLOAT constant such as 12.5E0 blocks all of it, and a
        // predicate an index could serve blocks that index (LIKE, BETWEEN). Unless --avoid-known-bugs is false, views
        // have no WHERE clause.
        if (!avoidKnownBugs && Randomly.getBoolean()) {
            VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(globalState).setColumns(table.getColumns());
            query.append(" WHERE ").append(VoltDBToStringVisitor.asString(gen.generateBooleanExpression()));
        }
        if (!grouping.isEmpty()) {
            query.append(" GROUP BY ")
                    .append(grouping.stream().map(VoltDBColumn::getName).collect(Collectors.joining(", ")));
        }
        String name = globalState.getFreeViewName();
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < outputs.size(); i++) {
            columns.add("C" + i);
        }
        globalState.addView(name, table.getName(), query.toString());
        ExpectedErrors errors = ExpectedErrors.newErrors().build();
        VoltDBErrors.addViewErrors(errors);
        return new SQLQueryAdapter("CREATE VIEW " + name + " (" + String.join(", ", columns) + ") AS " + query, errors,
                true);
    }

    // VoltDB sums TINYINT, SMALLINT and INTEGER values as BIGINT, which no realistic number of rows can overflow
    private static boolean sumCannotOverflow(VoltDBColumn c) {
        VoltDBDataType type = c.getType().getType();
        return type == VoltDBDataType.TINYINT || type == VoltDBDataType.SMALLINT || type == VoltDBDataType.INTEGER;
    }

}
