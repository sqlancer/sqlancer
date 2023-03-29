package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializePOSIXRegularExpression implements MaterializeExpression {

    private MaterializeExpression string;
    private MaterializeExpression regex;
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

    public MaterializePOSIXRegularExpression(MaterializeExpression string, MaterializeExpression regex, POSIXRegex op) {
        this.string = string;
        this.regex = regex;
        this.op = op;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        return null;
    }

    public MaterializeExpression getRegex() {
        return regex;
    }

    public MaterializeExpression getString() {
        return string;
    }

    public POSIXRegex getOp() {
        return op;
    }

}
