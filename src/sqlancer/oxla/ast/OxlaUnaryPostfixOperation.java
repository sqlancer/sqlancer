package sqlancer.oxla.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.List;

public class OxlaUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaUnaryPostfixOperation(OxlaExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }

    public static class OxlaUnaryPostfixOperator extends OxlaOperator {
        public OxlaUnaryPostfixOperator(String textRepresentation, OxlaTypeOverload overload) {
            super(textRepresentation, overload);
        }
    }

    // FIXME Imho, this class shouldn't hardcode the operators, but query the database for them instead.
    //       Sadly, Oxla does not support every pg_* table in the metastore so it's impossible for now.
    public static final List<OxlaOperator> ALL = List.of(
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.DATE, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.INTERVAL, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.JSON, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.TEXT, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.TIME, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.TIMESTAMP, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NULL", new OxlaTypeOverload(OxlaDataType.TIMESTAMPTZ, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.DATE, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.INTERVAL, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.JSON, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.TEXT, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.TIME, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.TIMESTAMP, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPostfixOperator("IS NOT NULL", new OxlaTypeOverload(OxlaDataType.TIMESTAMPTZ, OxlaDataType.BOOLEAN))
    );
    public static final OxlaOperator IS_NULL = ALL.get(0);
}
