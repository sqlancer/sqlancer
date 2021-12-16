package sqlancer.oceanbase.ast;

public class OceanBaseStringExpression implements OceanBaseExpression {

    private final String str;
    private final OceanBaseConstant expectedValue;

    public OceanBaseStringExpression(String str, OceanBaseConstant expectedValue) {
        this.str = str;
        this.expectedValue = expectedValue;
    }

    public String getStr() {
        return str;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        return expectedValue;
    }

}
