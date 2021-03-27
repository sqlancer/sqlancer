package sqlancer.h2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2CompositeDataType;
import sqlancer.h2.H2Schema.H2Table;

public class H2TableGenerator {

    public SQLQueryAdapter getQuery(H2GlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("already exists");
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(globalState.getSchema().getFreeTableName());
        sb.append("(");
        List<String> columnNames = new ArrayList<>();
        for (int i = 0; i < Randomly.fromOptions(1, 2, 3); i++) {
            columnNames.add("c" + i);
        }

        for (int i = 0; i < columnNames.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String c = columnNames.get(i);
            sb.append(c);
            sb.append(" ");
            sb.append(H2CompositeDataType.getRandom());
            boolean generated = Randomly.getBooleanWithRatherLowProbability();
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
            }
            if (Randomly.getBooleanWithRatherLowProbability() && !generated) {
                sb.append(" DEFAULT ");
                sb.append(H2ToStringVisitor.asString(new H2ExpressionGenerator(globalState).generateConstant()));
            }
            if (generated) {
                sb.append(" AS (");
                List<H2Column> columns = columnNames.stream().filter(cName -> !cName.contentEquals(c))
                        .map(c2 -> new H2Column(c2, null)).collect(Collectors.toList());
                H2ExpressionGenerator gen = new H2ExpressionGenerator(globalState).setColumns(columns);
                sb.append(H2ToStringVisitor.asString(gen.generateExpression()));
                H2Errors.addExpressionErrors(errors);
                errors.add("not found"); // generated column cycles
                sb.append(')');
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" SELECTIVITY ");
                sb.append(Randomly.getNotCachedInteger(0, 101));
            }
            if (Randomly.getBooleanWithRatherLowProbability() && !generated) {
                sb.append(" UNIQUE");
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" CHECK ");
                sb.append(H2ToStringVisitor.asString(new H2ExpressionGenerator(globalState)
                        .setColumns(columnNames.stream().map(c2 -> new H2Column(c2, null)).collect(Collectors.toList()))
                        .generateExpression()));
                H2Errors.addExpressionErrors(errors);
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(", PRIMARY KEY(");
            sb.append(Randomly.nonEmptySubset(columnNames).stream().collect(Collectors.joining(", ")));
            sb.append(")");
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<String> foreignKeyColumns = Randomly.nonEmptySubset(columnNames);
            sb.append(", FOREIGN KEY(");
            sb.append(foreignKeyColumns.stream().collect(Collectors.joining(", ")));
            sb.append(')');
            List<H2Table> foreignTableCandidates = globalState.getSchema().getDatabaseTables().stream()
                    .filter(t -> !t.isView()).collect(Collectors.toList());
            if (foreignTableCandidates.isEmpty()) {
                throw new IgnoreMeException();
            }
            H2Table foreignKeyTable = Randomly.fromList(foreignTableCandidates);
            sb.append(" REFERENCES ");
            sb.append(foreignKeyTable.getName());
            sb.append('(');
            if (foreignKeyTable.getColumns().size() < foreignKeyColumns.size()) {
                throw new IgnoreMeException();
            }
            sb.append(foreignKeyTable.getRandomNonEmptyColumnSubset(foreignKeyColumns.size()).stream()
                    .map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(')');
            if (Randomly.getBoolean()) {
                sb.append(" ON DELETE ");
                addReferentialAction(sb);
            }
            if (Randomly.getBoolean()) {
                sb.append(" ON UPDATE ");
                addReferentialAction(sb);
            }
            errors.add("are not comparable");
            errors.add(" cannot be updatable by a referential constraint with"); // generated columns
            errors.add("not found"); // Constraint "PRIMARY KEY | UNIQUE (C0)" not found;
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void addReferentialAction(StringBuilder sb) {
        sb.append(Randomly.fromOptions("CASCADE", "RESTRICT", "NO ACTION", "SET DEFAULT", "SET NULL"));
    }

}
