package sqlancer.clickhouse;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.MainOptions;
import sqlancer.TestOracle;
import sqlancer.clickhouse.oracle.tlp.ClickHouseTLPGroupByOracle;
import sqlancer.clickhouse.oracle.tlp.ClickHouseTLPWhereOracle;
import sqlancer.clickhouse.oracle.tlp.ClickHouseTLPHavingOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "ClickHouse")
public class ClickHouseOptions extends MainOptions {

    @Parameter(names = "--oracle", converter = MainOptions.DBMSConverter.class)
    public List<ClickHouseOracle> oracle = Arrays.asList(ClickHouseOracle.TLPWhere);

    @Parameter(names = { "--test-joins" }, description = "Allow the generation of JOIN clauses", arity = 1)
    public boolean testJoins = true;

    public enum ClickHouseOracle {
        TLPWhere {
            @Override
            public TestOracle create(ClickHouseProvider.ClickHouseGlobalState globalState) throws SQLException {
                return new ClickHouseTLPWhereOracle(globalState);
            }
        },
        TLPGroupBy {
            @Override
            public TestOracle create(ClickHouseProvider.ClickHouseGlobalState globalState) throws SQLException {
                return new ClickHouseTLPGroupByOracle(globalState);
            }
        },
        TLPHaving {
            @Override
            public TestOracle create(ClickHouseProvider.ClickHouseGlobalState globalState) throws SQLException {
                return new ClickHouseTLPHavingOracle(globalState);
            }
        };

        public abstract TestOracle create(ClickHouseProvider.ClickHouseGlobalState globalState) throws SQLException;
    }
}
