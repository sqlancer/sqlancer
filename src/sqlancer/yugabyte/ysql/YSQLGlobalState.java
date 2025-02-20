package sqlancer.yugabyte.ysql;

import java.sql.SQLException;

import sqlancer.ExpandedGlobalState;
import sqlancer.SQLConnection;

public class YSQLGlobalState extends ExpandedGlobalState<YSQLOptions, YSQLSchema> {

    @Override
    public void setConnection(SQLConnection con) {
        super.setConnection(con);
        try {
            this.opClasses = getOpclasses(getConnection());
            this.operators = getOperators(getConnection());
            this.collates = getCollnames(getConnection());
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public YSQLSchema readSchema() throws SQLException {
        return YSQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

}
