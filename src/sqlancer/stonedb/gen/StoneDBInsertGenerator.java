package sqlancer.stonedb.gen;

import sqlancer.mysql.gen.MySQLInsertGenerator;
import sqlancer.stonedb.StoneDBProvider;
import sqlancer.stonedb.StoneDBSchema;

public class StoneDBInsertGenerator extends MySQLInsertGenerator {

    private final StoneDBSchema.StoneDBTable table;

    public StoneDBInsertGenerator(StoneDBProvider.StoneDBGlobalState globalState) {
        super(globalState);
        // todo: fix this type cast
        table = (StoneDBSchema.StoneDBTable) globalState.getSchema().getRandomTable();
    }
}
