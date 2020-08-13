package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresPOSIXRegularExpression implements PostgresExpression {

    private PostgresExpression string;
    private PostgresExpression regex;
    private POSIXRegex op;

    public enum POSIXRegex implements Operator {
        MATCH_CASE_SENSITIVE("~"), MATCH_CASE_INSENSITIVE("~*"), NOT_MATCH_CASE_SENSITIVE("!~"),
        NOT_MATCH_CASE_INSENSITIVE("!~*");

        private String repr;

        POSIXRegex(String repr) {
            this.repr = repr;
        }

        public String getStringRepresentation() {
            return repr;
        }

        public static POSIXRegex getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public PostgresPOSIXRegularExpression(PostgresExpression string, PostgresExpression regex, POSIXRegex op) {
        this.string = string;
        this.regex = regex;
        this.op = op;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BOOLEAN;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        return null;
    }

    public PostgresExpression getRegex() {
        return regex;
    }

    public PostgresExpression getString() {
        return string;
    }

    public POSIXRegex getOp() {
        return op;
    }

}
