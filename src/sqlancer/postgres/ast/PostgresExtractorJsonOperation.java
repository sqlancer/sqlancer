package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.UnaryNode;
import sqlancer.postgres.PostgresSchema;

public class PostgresExtractorJsonOperation extends UnaryNode<PostgresExpression> implements PostgresExpression {

    private final String op;
    private final PostgresExtractorJsonOperator postgresExtractorJsonOperator;

    @Override
    public PostgresSchema.PostgresDataType getExpressionType() {
        return this.postgresExtractorJsonOperator.getType();
    }

    @Override
    public PostgresConstant getExpectedValue() {
        return PostgresExpression.super.getExpectedValue();
    }

    public enum PostgresExtractorJsonOperator {
        TEXT_EXTRACTOR("->>"){
            @Override
            public PostgresSchema.PostgresDataType getType(){
                return PostgresSchema.PostgresDataType.TEXT;
            }
        }, JSON_EXTRACTOR("->"){
            @Override
            public PostgresSchema.PostgresDataType getType(){
                return PostgresSchema.PostgresDataType.JSON;
            }
        };

        public abstract PostgresSchema.PostgresDataType getType();
        private final String textRepresentation;

        PostgresExtractorJsonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static PostgresExtractorJsonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public PostgresExtractorJsonOperation(PostgresExtractorJsonOperator op,
            PostgresExpression expr) {
        super(expr);
        this.op = op.getTextRepresentation();
        this.postgresExtractorJsonOperator = op;
    }

    @Override
    public String getOperatorRepresentation() {
        return this.op;
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return super.omitBracketsWhenPrinting();
    }

    @Override
    public OperatorKind getOperatorKind() {
        return null;
    }

}
