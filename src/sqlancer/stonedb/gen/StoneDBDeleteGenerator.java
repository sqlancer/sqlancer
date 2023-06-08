package sqlancer.stonedb.gen;

import sqlancer.mysql.gen.MySQLDeleteGenerator;
import sqlancer.stonedb.StoneDBProvider;

public class StoneDBDeleteGenerator extends MySQLDeleteGenerator {
    public StoneDBDeleteGenerator(StoneDBProvider.StoneDBGlobalState globalState) {
        super(globalState);
    }
}
