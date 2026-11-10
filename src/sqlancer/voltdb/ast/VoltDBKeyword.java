package sqlancer.voltdb.ast;

// A keyword argument of a function, written as is: the unit of DATEADD(DAY, n, t), TRUNCATE(MONTH, t),
// SINCE_EPOCH(SECOND, t) and TO_TIMESTAMP(SECOND, n). It is not a constant, so PARAMS never makes it a parameter.
public class VoltDBKeyword implements VoltDBExpression {

    private final String text;

    public VoltDBKeyword(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }

}
