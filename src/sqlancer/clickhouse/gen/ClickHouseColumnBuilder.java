package sqlancer.clickhouse.gen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseVisitor;

public class ClickHouseColumnBuilder {

    private final StringBuilder sb = new StringBuilder();

    private static boolean allowAlias = true;
    private static boolean allowMaterialized = true;
    private static boolean allowDefaultValue = true;
    private static boolean allowCodec = true;

    private enum Constraints {
        DEFAULT, MATERIALIZED, CODEC, ALIAS // TTL
    }

    public String createColumn(String columnName, ClickHouseProvider.ClickHouseGlobalState globalState,
            List<ClickHouseSchema.ClickHouseColumn> columns) {
        sb.append(columnName);
        sb.append(" ");
        List<Constraints> constraints = new ArrayList<>();
        ClickHouseSchema.ClickHouseLancerDataType dataType = ClickHouseSchema.ClickHouseLancerDataType.getRandom();
        if (Randomly.getBooleanWithSmallProbability()) {
            constraints = Randomly.subset(Constraints.values());
            if (!allowAlias || columns.isEmpty() || columns.size() == 1) {
                constraints.remove(Constraints.ALIAS);
            }
            if (!allowMaterialized) {
                constraints.remove(Constraints.MATERIALIZED);
            }
            if (!allowDefaultValue) {
                constraints.remove(Constraints.DEFAULT);
            }
            if (constraints.contains(Constraints.MATERIALIZED)) {
                constraints.remove(Constraints.ALIAS);
                constraints.remove(Constraints.DEFAULT);
            } else if (constraints.contains(Constraints.ALIAS)) {
                constraints.remove(Constraints.DEFAULT);
                constraints.remove(Constraints.CODEC);
            }
        }

        if (!constraints.contains(Constraints.ALIAS)) {
            sb.append(dataType);
        }

        Collections.sort(constraints);

        for (Constraints c : constraints) {
            switch (c) {
            case MATERIALIZED:
                if (allowMaterialized) {
                    sb.append(" MATERIALIZED (");
                    sb.append(
                            ClickHouseVisitor
                                    .asString(new ClickHouseExpressionGenerator(globalState)
                                            .setColumns(
                                                    columns.stream().filter(p -> !p.getName().contentEquals(columnName))
                                                            .collect(Collectors.toList()))
                                            .generateExpression(dataType)));
                    sb.append(")");
                }
                break;
            case DEFAULT:
                if (allowDefaultValue) {
                    sb.append(" DEFAULT ");
                    sb.append(new ClickHouseExpressionGenerator(globalState).generateConstant(dataType));
                }
                break;
            case ALIAS:
                if (allowAlias) {
                    sb.append(" ALIAS ");
                    sb.append(Randomly.fromList(columns.stream().filter(p -> !p.getName().contentEquals(columnName))
                            .collect(Collectors.toList())).getName());
                }
                break;
            case CODEC:
                if (allowCodec) {
                    sb.append(" CODEC (");
                    sb.append(Randomly.fromOptions("NONE", "ZSTD", "LZ4HC"));
                    sb.append(")");
                }
                break;
            default:
                throw new AssertionError();
            }
        }
        return sb.toString();
    }

}
