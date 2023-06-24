package sqlancer.reducer.VirtualDB;

import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.reducer.VirtualDB.VirtualDBOptions.VirtualDBFactory;

import java.util.ArrayList;
import java.util.List;

@Parameters(separators = "=", commandDescription = "VirtualDB (default port: " + "-1" + ", default host: " + "127.0.0.1"
        + ")")
public class VirtualDBOptions implements DBMSSpecificOptions<VirtualDBFactory> {

    List<VirtualDBFactory> factories = new ArrayList<>();

    @Override
    public List<VirtualDBFactory> getTestOracleFactory() {
        return factories;
    }

    public static class VirtualDBFactory implements OracleFactory<VirtualDBGlobalState> {
        @Override
        public TestOracle<VirtualDBGlobalState> create(VirtualDBGlobalState globalState) throws Exception {
            return null;
        }
    }
}
