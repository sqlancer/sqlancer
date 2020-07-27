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
    protected void updateSchema() throws SQLException {
        // FIXME: Will casting lose CitusSchema information?
        setSchema(CitusSchema.fromConnection(getConnection(), getDatabaseName()));
    }

}

 