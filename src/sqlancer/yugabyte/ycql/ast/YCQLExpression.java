package sqlancer.yugabyte.ycql.ast;

public interface YCQLExpression {

    default YCQLConstant getExpectedValue() {
        return null;
    }
}
