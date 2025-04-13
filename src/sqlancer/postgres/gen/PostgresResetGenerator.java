package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

public final class PostgresResetGenerator {
    private PostgresResetGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        PostgresSetGenerator.ConfigurationOption option = Randomly
                .fromOptions(PostgresSetGenerator.ConfigurationOption.values());
        sb.append("RESET ");
        if (Randomly.getBoolean()) {
            sb.append(option.getOptionName());
        } else {
            sb.append("ALL");
        }

        return new SQLQueryAdapter(sb.toString());
    }
}
