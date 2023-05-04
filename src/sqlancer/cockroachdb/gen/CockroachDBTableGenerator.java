package sqlancer.cockroachdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.common.query.SQLQueryAdapter;

public class CockroachDBTableGenerator extends CockroachDBGenerator {

    private final boolean primaryKey = Randomly.getBoolean();
    private final List<CockroachDBColumn> columns = new ArrayList<>();
    private boolean singleColumnPrimaryKey = primaryKey && Randomly.getBoolean();
    private final boolean compoundPrimaryKey = primaryKey && !singleColumnPrimaryKey;

    public CockroachDBTableGenerator(CockroachDBGlobalState globalState) {
        super(globalState);
        canAffectSchema = true;
    }

    public static SQLQueryAdapter generate(CockroachDBGlobalState globalState) {
        if (globalState.getSchema().getDatabaseTables().size() > globalState.getDbmsSpecificOptions().maxNumTables) {
            throw new IgnoreMeException();
        }
        return new CockroachDBTableGenerator(globalState).getQuery();
    }

    @Override
    public void buildStatement() {
        errors.add("and thus is not indexable"); // array types are not indexable
        errors.add("context-dependent operators are not allowed in STORED COMPUTED COLUMN");
        if (globalState.getDbmsSpecificOptions().testTempTables) {
            errors.add("constraints on temporary tables may reference only temporary tables");
            errors.add("constraints on permanent tables may reference only permanent tables");
        }
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE ");
        if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testTempTables) {
            sb.append("TEMP ");
        }
        sb.append("TABLE ");
        sb.append(tableName);
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = "c" + i;
            CockroachDBCompositeDataType columnType = CockroachDBCompositeDataType.getRandom();
            while (columnType.getPrimitiveDataType() == CockroachDBDataType.JSONB) {
                columnType = CockroachDBCompositeDataType.getRandom(); // TODO
            }
            columns.add(new CockroachDBColumn(columnName, columnType, false, false));

        }
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState).setColumns(columns);
        sb.append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            CockroachDBColumn cockroachDBColumn = columns.get(i);
            sb.append(cockroachDBColumn.getName());
            sb.append(" ");
            sb.append(cockroachDBColumn.getType());
            if (cockroachDBColumn.getType().isString() && Randomly.getBoolean()) {
                sb.append(" COLLATE " + CockroachDBCommon.getRandomCollate());
            }
            boolean generatedColumn = Randomly.getBooleanWithRatherLowProbability()
                    && cockroachDBColumn.getType().getPrimitiveDataType() != CockroachDBDataType.SERIAL;
            if (generatedColumn) {
                sb.append(" AS (");
                // To generate an expression exclude of the current column
                List<CockroachDBColumn> generatedColumns = new ArrayList<>(columns);
                generatedColumns.remove(i);
                CockroachDBExpressionGenerator genGeneratedColumn = new CockroachDBExpressionGenerator(globalState)
                        .setColumns(generatedColumns);
                sb.append(CockroachDBVisitor
                        .asString(genGeneratedColumn.generateExpression(cockroachDBColumn.getType())));
                sb.append(") STORED");
                errors.add("computed columns cannot reference other computed columns");
                errors.add("context-dependent operators are not allowed in computed column");
                errors.add("has type unknown");
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" UNIQUE ");
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL ");
            }
            if (singleColumnPrimaryKey && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" PRIMARY KEY");
                singleColumnPrimaryKey = false;
            }
            if (!generatedColumn && cockroachDBColumn.getType().getPrimitiveDataType() != CockroachDBDataType.SERIAL
                    && Randomly.getBoolean()) {
                sb.append(" DEFAULT (");
                sb.append(CockroachDBVisitor.asString(new CockroachDBExpressionGenerator(globalState)
                        .generateExpression(cockroachDBColumn.getType())));
                sb.append(")");
                errors.add("has type unknown"); // NULLIF
            }
            if (Randomly.getBooleanWithRatherLowProbability()
                    && !globalState.getSchema().getDatabaseTables().isEmpty()) {
                // TODO: also allow referencing itself
                sb.append(" REFERENCES ");
                CockroachDBTable otherTable = globalState.getSchema().getRandomTable();
                List<CockroachDBColumn> applicableColumns = otherTable.getColumns().stream()
                        .filter(c -> c.getType() == cockroachDBColumn.getType()).collect(Collectors.toList());
                if (applicableColumns.isEmpty()) {
                    throw new IgnoreMeException();
                }
                sb.append(otherTable.getName());
                sb.append("(");
                sb.append(Randomly.fromList(applicableColumns).getName());
                sb.append(")");
                if (Randomly.getBoolean()) {
                    sb.append(" MATCH ");
                    sb.append(Randomly.fromOptions("SIMPLE", "FULL"));
                }
                if (Randomly.getBoolean()) {
                    errors.add("cannot add a SET DEFAULT cascading action on column");
                    errors.add("cannot add a SET NULL cascading action on column ");
                    List<String> options = Randomly.nonEmptySubset("UPDATE", "DELETE");
                    for (String s : options) {
                        sb.append(" ON ");
                        sb.append(s);
                        sb.append(" ");
                        sb.append(Randomly.fromOptions("CASCADE", "SET NULL", "SET DEFAULT"));
                    }
                }
                errors.add("there is no unique constraint matching given keys for referenced table");
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                errors.add("has type unknown");
                sb.append(" CHECK (");
                sb.append(CockroachDBVisitor.asString(gen.generateExpression(CockroachDBDataType.BOOL.get())));
                sb.append(")");
            }
        }
        if (compoundPrimaryKey) {
            sb.append(", CONSTRAINT \"primary\" PRIMARY KEY");
            List<CockroachDBColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
            addColumns(sb, primaryKeyColumns, true);
        }
        if (Randomly.getBoolean()) {
            sb.append(", FAMILY \"primary\" (");
            sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(")");
        errors.add("collatedstring");
        CockroachDBErrors.addExpressionErrors(errors);
    }

}
