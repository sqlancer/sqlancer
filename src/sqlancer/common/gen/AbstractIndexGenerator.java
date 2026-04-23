package sqlancer.common.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.schema.AbstractTableColumn;

public abstract class AbstractIndexGenerator<C extends AbstractTableColumn<?, ?>> extends AbstractGenerator {

    protected void appendCreateIndex(boolean unique) {
        sb.append("CREATE ");
        if (unique) {
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ");
    }

    protected void appendIndexColumnList(List<C> columns, boolean allowOrdering) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            if (allowOrdering && Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
    }

}
