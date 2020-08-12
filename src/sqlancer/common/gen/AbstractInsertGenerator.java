package sqlancer.common.gen;

import java.util.List;

import sqlancer.Randomly;

public abstract class AbstractInsertGenerator<C> {

    protected StringBuilder sb = new StringBuilder();

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

    protected abstract void insertValue(C tiDBColumn);

}
