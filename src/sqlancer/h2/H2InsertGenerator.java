package sqlancer.h2;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Column;
import sqlancer.h2.H2Schema.H2Table;

public class H2InsertGenerator extends AbstractInsertGenerator<H2Column> {

    private final H2GlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();
    private final H2ExpressionGenerator gen;

    public H2InsertGenerator(H2GlobalState globalState) {
        this.globalState = globalState;
        gen = new H2ExpressionGenerator(globalState);
    }

    public static SQLQueryAdapter getQuery(H2GlobalState globalState) {
        return new H2InsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        boolean mergeInto = false; // Randomly.getBooleanWithRatherLowProbability();
        if (mergeInto) {
            sb.append("MERGE INTO ");
            errors.add("Index \"PRIMARY_KEY_\" not found");
            errors.add("contains null values");
            errors.add("Valid MERGE INTO statement with at least one updatable column");
        } else {
            sb.append("INSERT INTO ");
        }
        H2Table table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<H2Column> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        if (mergeInto && Randomly.getBoolean()) {
            sb.append(" KEY(");
            sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(" VALUES ");
        insertColumns(columns);
        H2Errors.addInsertErrors(errors);
        H2Errors.addExpressionErrors(errors); // generated columns
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(H2Column tiDBColumn) {
        sb.append(H2ToStringVisitor.asString(gen.generateConstant()));
    }
}
