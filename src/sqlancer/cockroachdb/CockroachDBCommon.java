package sqlancer.cockroachdb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBIndexReference;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;

public final class CockroachDBCommon {

    private CockroachDBCommon() {
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("en", "de", "es", "cmn");
    }

    public static List<CockroachDBExpression> getTableReferences(List<CockroachDBTableReference> tableList) {
        List<CockroachDBExpression> from = new ArrayList<>();
        for (CockroachDBTableReference t : tableList) {
            CockroachDBTable table = t.getTable();
            if (!table.getIndexes().isEmpty() && Randomly.getBooleanWithSmallProbability()) {
                from.add(new CockroachDBIndexReference(t, table.getRandomIndex()));
            } else {
                from.add(t);
            }
        }
        return from;
    }

}
