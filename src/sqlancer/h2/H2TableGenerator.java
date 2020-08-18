package sqlancer.h2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2CompositeDataType;

public class H2TableGenerator {

    public Query getQuery(H2GlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("already exists");
        StringBuilder sb = new StringBuilder("CREATE TABLE " + globalState.getSchema().getFreeTableName() + "(");
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
            if (Randomly.getBooleanWithRatherLowProbability() && !generated) {
                sb.append(" UNIQUE");
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
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
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(", PRIMARY KEY(");
            sb.append(Randomly.nonEmptySubset(columnNames).stream().collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(")");
        return new QueryAdapter(sb.toString(), errors, true);
    }

}
