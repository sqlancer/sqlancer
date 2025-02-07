package sqlancer.clickhouse.oracle.tlp;

import java.sql.SQLException;

import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ast.ClickHouseSelect;

public class ClickHouseTLPDistinctOracle extends ClickHouseTLPBase {

    public ClickHouseTLPDistinctOracle(ClickHouseProvider.ClickHouseGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setSelectType(ClickHouseSelect.SelectType.DISTINCT);
        executeAndCompare(select, false);
    }

}
