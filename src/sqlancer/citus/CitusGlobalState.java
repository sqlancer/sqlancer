package sqlancer.citus;

import java.sql.SQLException;

import sqlancer.postgres.PostgresGlobalState;

public class CitusGlobalState extends PostgresGlobalState {

    private boolean repartition;

    public void setRepartition(boolean repartition) {
        this.repartition = repartition;
    }

    public boolean getRepartition() {
        return repartition;
    }

    @Override
    public void updateSchema() throws SQLException {
        setSchema(CitusSchema.fromConnection(getConnection(), getDatabaseName()));
    }

}
