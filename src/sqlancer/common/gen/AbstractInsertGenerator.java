package sqlancer.common.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractTableColumn;

public abstract class AbstractInsertGenerator<C extends AbstractTableColumn<?, ?>> {

    protected StringBuilder sb = new StringBuilder();
    protected ExpectedErrors errors = new ExpectedErrors();

    protected void appendColumnList(List<C> columns) {
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
    }

    protected void buildInsertInto(String tableName, List<C> columns) {
        sb.append("INSERT INTO ");
        sb.append(tableName);
        appendColumnList(columns);
        sb.append(" VALUES ");
        insertColumns(columns);
    }

    protected void insertColumns(List<C> columns) {
        for (int nrRows = 0; nrRows < Randomly.smallNumber() + 1; nrRows++) {
            if (nrRows != 0) {
                sb.append(", ");
            }
            sb.append("(");
            for (int nrColumn = 0; nrColumn < columns.size(); nrColumn++) {
                if (nrColumn != 0) {
                    sb.append(", ");
                }
                insertValue(columns.get(nrColumn));
            }
            sb.append(")");
        }
    }

    protected abstract void insertValue(C column);

}
