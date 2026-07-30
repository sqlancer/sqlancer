package sqlancer.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TestPostgresProvider {

    @Test
    void createTablespaceIsNotScheduledWhenDisabled() {
        PostgresGlobalState state = new PostgresGlobalState();
        state.setDbmsSpecificOptions(new PostgresOptions() {
            @Override
            public boolean isTestTablespaces() {
                return false;
            }
        });

        assertEquals(0, PostgresProvider.mapActions(state, PostgresProvider.Action.CREATE_TABLESPACE));
    }
}
