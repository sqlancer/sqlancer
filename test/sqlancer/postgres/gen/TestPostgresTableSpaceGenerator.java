package sqlancer.postgres.gen;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import sqlancer.IgnoreMeException;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;

class TestPostgresTableSpaceGenerator {

    @Test
    void generateIsSkippedWhenTablespacesAreDisabled() {
        PostgresGlobalState state = new PostgresGlobalState();
        state.setDbmsSpecificOptions(new PostgresOptions() {
            @Override
            public boolean isTestTablespaces() {
                return false;
            }
        });

        assertThrows(IgnoreMeException.class, () -> PostgresTableSpaceGenerator.generate(state));
    }
}
