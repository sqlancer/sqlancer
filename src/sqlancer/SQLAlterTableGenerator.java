package sqlancer;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.postgres.gen.PostgresAlterTableGenerator;

import java.util.List;

public abstract class SQLAlterTableGenerator<T extends AbstractRelationalTable<?, ?, ?>, G extends ExpandedGlobalState<?, ?>, A extends Enum<A>> {

    protected final T randomTable;
    protected final G globalState;
    protected final Randomly r;

    public SQLAlterTableGenerator(T randomTable, G globalState) {
        this.randomTable = randomTable;
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public abstract List<A> getActions(ExpectedErrors errors);

    public SQLQueryAdapter generate() {
        ExpectedErrors errors = new ExpectedErrors();
        int i = 0;
        List<A> action = getActions(errors);
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        if (Randomly.getBoolean()) {
            sb.append(" ONLY");
            errors.add("cannot use ONLY for foreign key on partitioned table");
        }
        sb.append(" ");
        sb.append(randomTable.getName());
        sb.append(" ");
        for (A a : action) {
            if (i++ != 0) {
                sb.append(", ");
            }
            switch(a.name()) {
                case "ALTER_TABLE_DROP_COLUMN":

            }
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }


}
