package sqlancer.mariadb.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.gen.AbstractIndexGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

public class MariaDBIndexGenerator extends AbstractIndexGenerator<MariaDBColumn> {

    private final MariaDBSchema schema;

    public MariaDBIndexGenerator(MariaDBSchema schema) {
        this.schema = schema;
        this.canAffectSchema = true;
    }

    public static SQLQueryAdapter generate(MariaDBSchema s) {
        return new MariaDBIndexGenerator(s).getStatement();
    }

    @Override
    public void buildStatement() {
        errors.add("Key/Index cannot be defined on a virtual generated column");
        errors.add("Specified key was too long");
        boolean unique = Randomly.getBoolean();
        if (unique) {
            errors.add("Duplicate entry");
            errors.add("Key/Index cannot be defined on a virtual generated column");
        }
        appendCreateIndex(unique);
        sb.append("i");
        sb.append(DBMSCommon.createColumnName(Randomly.smallNumber()));
        if (Randomly.getBoolean()) {
            sb.append(" USING ");
            sb.append(Randomly.fromOptions("BTREE", "HASH")); // , "RTREE")
        }

        sb.append(" ON ");
        MariaDBTable randomTable = schema.getRandomTable();
        sb.append(randomTable.getName());
        appendIndexColumnList(Randomly.nonEmptySubset(randomTable.getColumns()), true);
    }

}
