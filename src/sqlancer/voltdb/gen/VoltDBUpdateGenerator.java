package sqlancer.voltdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.voltdb.VoltDBErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.VoltDBToStringVisitor;
import sqlancer.voltdb.ast.VoltDBCase;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBFunction;
import sqlancer.voltdb.ast.VoltDBFunction.VoltDBFunctionName;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator.Kind;

/**
 * UPDATE of one table: one to three columns, each set to a constant of its type or to an expression of its kind over
 * the table's columns, usually with a WHERE clause. The partitioning column is never set: VoltDB does not support
 * updating it ("Updating a partitioning column is not supported").
 */
public final class VoltDBUpdateGenerator {

    private VoltDBUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState) {
        return getQuery(globalState, globalState.getSchema().getRandomTable());
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState, VoltDBTable table) {
        List<VoltDBColumn> settable = table.getColumns().stream()
                .filter(c -> !c.getName().equals(table.getPartitionColumn())).collect(Collectors.toList());
        if (settable.isEmpty()) {
            throw new IgnoreMeException();
        }
        VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(globalState).setColumns(table.getColumns());
        Randomly r = globalState.getRandomly();
        List<String> assignments = new ArrayList<>();
        List<VoltDBColumn> targets = Randomly.nonEmptySubset(settable);
        for (VoltDBColumn c : targets.subList(0, Math.min(3, targets.size()))) {
            String value;
            Kind kind = Kind.fromType(c.getType().getType());
            if (kind == Kind.BOOLEAN && globalState.getDbmsSpecificOptions().avoidKnownBugs) {
                // ENG-30494: a BOOLEAN literal in SET takes another assignment's or the WHERE clause's parameter
                // ("Type BOOLEAN can't be cast as BIGINT"), so it is written as a comparison of integers
                value = VoltDBToStringVisitor.asString(gen.generateConstant(kind));
            } else if (Randomly.getBoolean() || c.getType().getType() == VoltDBDataType.DATE
                    && globalState.getDbmsSpecificOptions().avoidKnownBugs) {
                // ENG-30561: a TIMESTAMP constant assigned to a DATE column fails to plan ("has an invalid format for
                // a constant date value"), and temporal expressions use TIMESTAMP constants (ENG-30560), so a DATE
                // column is set to a date string
                value = VoltDBConstantGenerator.generate(r, c.getType(), c.isNullable());
            } else {
                VoltDBExpression expr = gen.generateExpression(kind);
                // ENG-30589: setting a VARCHAR column to a CASE or COALESCE fails to compile ("Asking for fixed size
                // for non-fixed or unknown type:varchar"); inside another function it works
                while (kind == Kind.STRING && globalState.getDbmsSpecificOptions().avoidKnownBugs
                        && (expr instanceof VoltDBCase || expr instanceof VoltDBFunction
                                && ((VoltDBFunction) expr).getFunc() == VoltDBFunctionName.COALESCE)) {
                    expr = gen.generateExpression(kind);
                }
                value = VoltDBToStringVisitor.asString(expr);
            }
            assignments.add(c.getName() + " = " + value);
        }
        StringBuilder sql = new StringBuilder("UPDATE ").append(table.getName()).append(" SET ")
                .append(String.join(", ", assignments));
        if (!Randomly.getBooleanWithRatherLowProbability()) {
            sql.append(" WHERE ").append(VoltDBToStringVisitor.asString(gen.generateBooleanExpression()));
        }
        ExpectedErrors errors = ExpectedErrors.newErrors().build();
        VoltDBErrors.addExpressionErrors(errors);
        VoltDBErrors.addUpdateErrors(errors);
        return new SQLQueryAdapter(sql.toString(), errors);
    }

}
