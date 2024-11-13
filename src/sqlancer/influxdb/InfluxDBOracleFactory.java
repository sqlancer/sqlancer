package sqlancer.influxdb;

import java.sql.SQLException;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.influxdb.test.InfluxDBQueryPartitioningWhereTester;

public enum InfluxDBOracleFactory implements OracleFactory<InfluxDBProvider.InfluxDBGlobalState> {
    DEFAULT, WHERE {
        @Override
        public TestOracle<InfluxDBProvider.InfluxDBGlobalState> create(InfluxDBProvider.InfluxDBGlobalState globalState)
                throws SQLException {
            return new InfluxDBQueryPartitioningWhereTester(globalState);
        }
    }
}