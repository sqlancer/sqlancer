package sqlancer.voltdb.ast;

// A literal, written out as VoltDB SQL (see VoltDBConstantGenerator).
public class VoltDBConstant implements VoltDBExpression {

    private final String text;

    public VoltDBConstant(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }

}
