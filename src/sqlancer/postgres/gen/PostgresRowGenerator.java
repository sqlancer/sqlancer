package sqlancer.postgres.gen;

import java.util.List;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresExpression;

public class PostgresRowGenerator implements PostgresExpression {
    private final List<PostgresExpression> expressions;
    
    public PostgresRowGenerator(List<PostgresExpression> expressions) {
        this.expressions = expressions;
    }
    
    public List<PostgresExpression> getExpressions() {
        return expressions;
    }
    
    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.INT;
    }
}