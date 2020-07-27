package sqlancer.citus;

import java.sql.SQLException;

import sqlancer.postgres.PostgresGlobalState;

public class CitusGlobalState extends PostgresGlobalState<CitusOptions, CitusSchema> {

    private boolean repartition;
    
    public void setRepartition(boolean repartition) {
        this.repartition = repartition;
    }

    public boolean getRepartition() {
        return repartition;
    }

    // TODO: What if this doesn't exist - why can't the function in PostgresGlobalState call S.fromConnection()?
    @Override
    protected void updateSchema() throws SQLException {
        // FIXME: Will casting lose CitusSchema information?
        setSchema(CitusSchema.fromConnection(getConnection(), getDatabaseName()));
    }

}

 