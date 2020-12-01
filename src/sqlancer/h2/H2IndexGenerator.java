package sqlancer.h2;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2Table;

public class H2IndexGenerator {

    private final H2GlobalState globalState;

    public H2IndexGenerator(H2GlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(H2GlobalState globalState) {
        return new H2IndexGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (Randomly.getBoolean()) {
            sb.append("UNIQUE ");
            errors.add("Unique index or primary key violation");
        }
        if (Randomly.getBoolean()) {
            sb.append("HASH ");
        }
        sb.append("INDEX IF NOT EXISTS ");
        sb.append(globalState.getSchema().getFreeIndexName());
        sb.append(" ON ");
        H2Table table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        sb.append('(');
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(table.getRandomColumn().getName());
            if (Randomly.getBoolean()) {
                sb.append(' ');
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
            if (Randomly.getBoolean()) {
                sb.append(" NULLS ");
                sb.append(Randomly.fromOptions("FIRST", "LAST"));
            }
        }
        sb.append(')');
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
