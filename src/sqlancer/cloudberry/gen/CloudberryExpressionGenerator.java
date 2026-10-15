package sqlancer.cloudberry.gen;

import java.util.List;

import sqlancer.cloudberry.oracle.CloudberryOracleUtils;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class CloudberryExpressionGenerator extends PostgresExpressionGenerator {

    public CloudberryExpressionGenerator(PostgresGlobalState globalState) {
        super(globalState);
    }

    @Override
    public List<PostgresJoin> getRandomJoinClauses() {
        return CloudberryOracleUtils.removeOnlyFromJoins(super.getRandomJoinClauses());
    }

    @Override
    public List<PostgresExpression> getTableRefs() {
        return CloudberryOracleUtils.removeOnlyFromTables(super.getTableRefs());
    }

}
