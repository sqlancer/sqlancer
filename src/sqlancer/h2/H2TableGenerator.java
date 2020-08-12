package sqlancer.h2;

import sqlancer.Randomly;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.h2.H2Provider.H2GlobalState;
import sqlancer.h2.H2Schema.H2CompositeDataType;

public class H2TableGenerator {

    public Query getQuery(H2GlobalState globalState) {
        StringBuilder sb = new StringBuilder("CREATE TABLE t0(");
        for (int i = 0; i < Randomly.fromOptions(1, 2, 3); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("c");
            sb.append(i);
            sb.append(" ");
            sb.append(H2CompositeDataType.getRandom());
        }
        sb.append(")");
        return new QueryAdapter(sb.toString(), true);
    }

}
