package sqlancer.oceanbase.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;

public class OceanBaseAlterTable {

    private final OceanBaseSchema schema;
    private final StringBuilder sb = new StringBuilder();
    boolean couldAffectSchema;
    private List<Action> selectedActions;

    public OceanBaseAlterTable(OceanBaseSchema newSchema) {
        this.schema = newSchema;
    }

    public static SQLQueryAdapter create(OceanBaseGlobalState globalState) {
        return new OceanBaseAlterTable(globalState.getSchema()).create();
    }

    private enum Action {
        COMPRESSION;

        private String[] potentialErrors;

        Action(String... couldCauseErrors) {
            this.potentialErrors = couldCauseErrors.clone();
        }

    }

    private SQLQueryAdapter create() {
        ExpectedErrors errors = ExpectedErrors.from("does not support the create option", "doesn't have this option",
                "is not supported for this operation", "Data truncation", "Specified key was too long");
        errors.add("Data truncated for functional index ");
        sb.append("ALTER TABLE ");
        OceanBaseTable table = schema.getRandomTable();
        sb.append(table.getName());
        sb.append(" ");
        List<Action> list = new ArrayList<>(Arrays.asList(Action.values()));
        selectedActions = Randomly.subset(list);
        int i = 0;
        for (Action a : selectedActions) {
            if (i++ != 0) {
                sb.append(", ");
            }
            switch (a) {
            case COMPRESSION:
                sb.append("COMPRESSION ");
                sb.append("'");
                sb.append(Randomly.fromOptions("ZLIB_1.0", "LZ4_1.0", "NONE"));
                sb.append("'");
                break;
            default:
                break;
            }
        }
        for (Action a : selectedActions) {
            for (String error : a.potentialErrors) {
                errors.add(error);
            }
        }
        return new SQLQueryAdapter(sb.toString(), errors, couldAffectSchema);
    }

}
