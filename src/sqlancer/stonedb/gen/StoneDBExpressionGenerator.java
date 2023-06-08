package sqlancer.stonedb.gen;

import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.stonedb.StoneDBProvider;

public class StoneDBExpressionGenerator extends MySQLExpressionGenerator {
    private final StoneDBProvider.StoneDBGlobalState state;

    public StoneDBExpressionGenerator(StoneDBProvider.StoneDBGlobalState state) {
        super(state);
        this.state = state;
    }
}
