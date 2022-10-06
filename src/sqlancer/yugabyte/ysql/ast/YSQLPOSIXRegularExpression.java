package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLPOSIXRegularExpression implements YSQLExpression {

    private final YSQLExpression string;
    private final YSQLExpression regex;
    private final POSIXRegex op;

    public YSQLPOSIXRegularExpression(YSQLExpression string, YSQLExpression regex, POSIXRegex op) {
        this.string = string;
        this.regex = regex;
        this.op = op;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.BOOLEAN;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        return null;
    }

    public YSQLExpression getRegex() {
        return regex;
    }

    public YSQLExpression getString() {
        return string;
    }

    public POSIXRegex getOp() {
        return op;
    }

    public enum POSIXRegex implements Operator {
        MATCH_CASE_SENSITIVE("~"), MATCH_CASE_INSENSITIVE("~*"), NOT_MATCH_CASE_SENSITIVE("!~"),
        NOT_MATCH_CASE_INSENSITIVE("!~*");

        private final String repr;

        POSIXRegex(String repr) {
            this.repr = repr;
        }

        public static POSIXRegex getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getStringRepresentation() {
            return repr;
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

}
