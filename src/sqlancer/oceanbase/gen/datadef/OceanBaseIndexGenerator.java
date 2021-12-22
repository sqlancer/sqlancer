package sqlancer.oceanbase.gen.datadef;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oceanbase.OceanBaseErrors;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseDataType;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;

public class OceanBaseIndexGenerator {

    private final Randomly r;
    private StringBuilder sb = new StringBuilder();
    private OceanBaseSchema schema;
    private final List<OceanBaseColumn> columns = new ArrayList<>();
    private final OceanBaseGlobalState globalState;

    public OceanBaseIndexGenerator(OceanBaseSchema schema, Randomly r, OceanBaseGlobalState globalState) {
        this.schema = schema;
        this.r = r;
        this.globalState = globalState;
    }

    public static SQLQueryAdapter create(OceanBaseGlobalState globalState) {
        return new OceanBaseIndexGenerator(globalState.getSchema(), globalState.getRandomly(), globalState).create();
    }

    public SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();
        OceanBaseErrors.addExpressionErrors(errors);
        sb.append("CREATE ");
        sb.append("INDEX ");
        sb.append(globalState.getSchema().getFreeIndexName());
        indexType();
        sb.append(" ON ");
        OceanBaseTable table = schema.getRandomTable();
        sb.append(table.getName());
        sb.append("(");
        List<OceanBaseColumn> randomColumn = table.getRandomNonEmptyColumnSubset();
        int i = 0;
        for (OceanBaseColumn c : randomColumn) {
            if (i++ != 0) {
                sb.append(", ");
            }
            c.isPartioned = true;
            columns.add(c);
            sb.append(c.getName());
            if (Randomly.getBoolean() && c.getType() == OceanBaseDataType.VARCHAR) {
                sb.append("(");
                sb.append(r.getInteger(1, 5));
                sb.append(")");
                c.isPartioned = false;
            }
        }
        sb.append(")");
        appendPartitionOptions();
        indexOption();
        String string = sb.toString();
        sb = new StringBuilder();
        errors.add("A primary key index cannot be invisible");
        errors.add("Functional index on a column is not supported. Consider using a regular index instead.");
        errors.add("Incorrect usage of spatial/fulltext/hash index and explicit index order");
        errors.add("must include all columns");
        errors.add("cannot index the expression");
        errors.add("Data truncation: Truncated incorrect");
        errors.add("a disallowed function.");
        errors.add("Data truncation");
        errors.add("Cannot create a functional index on an expression that returns a BLOB or TEXT.");
        errors.add("used in key specification without a key length");
        errors.add("can't be used in key specification with the used table type");
        errors.add("Specified key was too long");
        errors.add("out of range");
        errors.add("Data truncated for functional index");
        errors.add("used in key specification without a key length");
        errors.add("Row size too large"); // seems to happen together with MIN_ROWS in the table declaration
        return new SQLQueryAdapter(string, errors, true);
    }

    private void appendPartitionOptions() {
        if (Randomly.getBoolean()) {
            return;
        }

        OceanBaseColumn colIndex = Randomly.fromList(columns);

        if (!colIndex.isPartioned) {
            return;
        }

        if (colIndex.getType() == OceanBaseDataType.VARCHAR) {
            sb.append(" PARTITION BY");
            sb.append(" KEY");
            sb.append(" (");
            String name = colIndex.getName();
            sb.append(name);
            sb.append(")");
            sb.append(" partitions ");
            sb.append(r.getInteger(1, 20));
        } else if (OceanBaseDataType.INT == colIndex.getType()) {
            sb.append(" PARTITION BY");
            sb.append(" HASH(");
            String name = colIndex.getName();
            sb.append(name);
            sb.append(") ");
            sb.append(" partitions ");
            sb.append(r.getInteger(1, 20));
        } else {
            return;
        }
    }

    private void indexOption() {
        if (Randomly.getBoolean()) {
            sb.append(" ");
        }
    }

    private void indexType() {
        if (Randomly.getBoolean()) {
            sb.append(" USING ");
            sb.append(Randomly.fromOptions("BTREE", "HASH"));
        }
    }

    public void setNewSchema(OceanBaseSchema schema) {
        this.schema = schema;
    }
}
