package sqlancer.timescaledb;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;
import sqlancer.timescaledb.gen.TimeScaleDBCommon;

@AutoService(DatabaseProvider.class)
public class TimeScaleDBProvider extends PostgresProvider {
    @SuppressWarnings("unchecked")
    public TimeScaleDBProvider() {
        super((Class<PostgresGlobalState>) (Object) TimeScaleDBGlobalState.class,
                (Class<PostgresOptions>) (Object) TimeScaleDBOptions.class);
    }

    @Override
    public String getDBMSName() {
        getTimeScaleDBErrors(); // todo: remove this
        return "TimeScaleDB";
    }

    private static ExpectedErrors getTimeScaleDBErrors() {
        ExpectedErrors errors = new ExpectedErrors();
        TimeScaleDBCommon.addTimeScaleDBErrors(errors);
        return errors;
    }
}
