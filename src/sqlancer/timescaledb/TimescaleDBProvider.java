package sqlancer.timescaledb;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;
import sqlancer.timescaledb.gen.TimescaleDBCommon;

@AutoService(DatabaseProvider.class)
public class TimescaleDBProvider extends PostgresProvider {
    @SuppressWarnings("unchecked")
    public TimescaleDBProvider() {
        super((Class<PostgresGlobalState>) (Object) TimescaleDBGlobalState.class,
                (Class<PostgresOptions>) (Object) TimescaleDBOptions.class);
    }

    @Override
    public String getDBMSName() {
        return "timescaledb";
    }

    public static ExpectedErrors getTimescaleDBErrors() {
        ExpectedErrors errors = new ExpectedErrors();
        TimescaleDBCommon.addTimescaleDBErrors(errors);
        return errors;
    }
}
