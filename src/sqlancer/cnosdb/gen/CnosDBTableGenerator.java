package sqlancer.cnosdb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.cnosdb.CnosDBSchema.CnosDBFieldColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTable;
import sqlancer.cnosdb.CnosDBSchema.CnosDBTagColumn;
import sqlancer.cnosdb.query.CnosDBOtherQuery;
import sqlancer.common.query.ExpectedErrors;

public class CnosDBTableGenerator {

    protected final ExpectedErrors errors = new ExpectedErrors();
    private final String tableName;
    private final StringBuilder sb = new StringBuilder();
    private final List<CnosDBColumn> columnsToBeAdd = new ArrayList<>();
    private CnosDBTable table;

    public CnosDBTableGenerator(String tableName) {
        this.tableName = tableName;
    }

    public static CnosDBOtherQuery generate(String tableName) {
        return new CnosDBTableGenerator(tableName).generate();
    }

    protected CnosDBOtherQuery generate() {
        table = new CnosDBTable(tableName, columnsToBeAdd);

        sb.append("CREATE TABLE");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ");
        sb.append(tableName);

        sb.append("(");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String name = String.format("f%d", i);
            createField(name);
            sb.append(", ");
        }

        sb.append("TAGS(");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String name = String.format("t%d", i);
            createTag(name);
        }
        sb.append("))");
        return new CnosDBOtherQuery(sb.toString(), new ExpectedErrors());
    }

    private void createField(String name) throws AssertionError {
        sb.append(name);
        sb.append(" ");
        CnosDBDataType type = CnosDBDataType.getRandomTypeWithoutTimeStamp();
        CnosDBCommon.appendDataType(type, sb);
        CnosDBFieldColumn c = new CnosDBFieldColumn(name, type);
        c.setTable(table);
        sb.append(" ");
        columnsToBeAdd.add(c);
    }

    private void createTag(String name) {
        sb.append(name);
        CnosDBColumn column = new CnosDBTagColumn(name);
        column.setTable(table);
        columnsToBeAdd.add(column);
    }
}
