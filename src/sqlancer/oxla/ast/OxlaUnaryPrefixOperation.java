package sqlancer.oxla.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.oxla.schema.OxlaDataType;

import java.util.List;
import java.util.stream.Collectors;

public class OxlaUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<OxlaExpression>
        implements OxlaExpression {
    public OxlaUnaryPrefixOperation(OxlaExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }

    public static class OxlaUnaryPrefixOperator extends OxlaOperator {
        public OxlaUnaryPrefixOperator(String textRepresentation, OxlaTypeOverload overload) {
            super(textRepresentation, overload);
        }
    }

    // FIXME Imho, this class shouldn't hardcode the operators, but query the database for them instead.
    //       Sadly, Oxla does not support every pg_* table in the metastore so it's impossible for now.
    public static final List<OxlaOperator> ALL = List.of(
            new OxlaUnaryPrefixOperator("+", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT32)),
            new OxlaUnaryPrefixOperator("+", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64)),
            new OxlaUnaryPrefixOperator("+", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.INT32)),
            new OxlaUnaryPrefixOperator("+", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.INT64)),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT32)),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64)),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.INT32)),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.INT64)),
            new OxlaUnaryPrefixOperator("-", new OxlaTypeOverload(OxlaDataType.INTERVAL, OxlaDataType.INTERVAL)),
            new OxlaUnaryPrefixOperator("@", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT32)),
            new OxlaUnaryPrefixOperator("@", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64)),
            new OxlaUnaryPrefixOperator("@", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.INT32)),
            new OxlaUnaryPrefixOperator("@", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.INT64)),
            new OxlaUnaryPrefixOperator("NOT", new OxlaTypeOverload(OxlaDataType.BOOLEAN, OxlaDataType.BOOLEAN)),
            new OxlaUnaryPrefixOperator("|/", new OxlaTypeOverload(OxlaDataType.FLOAT32, OxlaDataType.FLOAT64)),
            new OxlaUnaryPrefixOperator("||/", new OxlaTypeOverload(OxlaDataType.FLOAT64, OxlaDataType.FLOAT64)),
            new OxlaUnaryPrefixOperator("~", new OxlaTypeOverload(OxlaDataType.INT32, OxlaDataType.INT32)),
            new OxlaUnaryPrefixOperator("~", new OxlaTypeOverload(OxlaDataType.INT64, OxlaDataType.INT64))
//            new OxlaUnaryPrefixOperator("~", new OxlaTypeOverload(OxlaDataType.TEXT, OxlaDataType.TEXT)) // FIXME Generate only for REGEXes.
    );
    public static final OxlaOperator NOT = ALL.get(13);

    public static List<OxlaOperator> getForType(OxlaDataType returnType) {
        return ALL.stream()
                .filter(op -> (op.overload.returnType == returnType))
                .collect(Collectors.toList());
    }
}
