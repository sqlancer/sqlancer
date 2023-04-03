package sqlancer.materialize.ast;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeAggregate.MaterializeAggregateFunction;

/**
 * @see <a href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</a>
 */
public class MaterializeAggregate extends FunctionNode<MaterializeAggregateFunction, MaterializeExpression>
        implements MaterializeExpression {

    public enum MaterializeAggregateFunction {
        AVG(MaterializeDataType.INT, MaterializeDataType.FLOAT, MaterializeDataType.REAL, MaterializeDataType.DECIMAL),
        BIT_AND(MaterializeDataType.INT), BIT_OR(MaterializeDataType.INT), BOOL_AND(MaterializeDataType.BOOLEAN),
        BOOL_OR(MaterializeDataType.BOOLEAN), COUNT(MaterializeDataType.INT), MAX, MIN,
        SUM(MaterializeDataType.INT, MaterializeDataType.FLOAT, MaterializeDataType.REAL, MaterializeDataType.DECIMAL);

        private MaterializeDataType[] supportedReturnTypes;

        MaterializeAggregateFunction(MaterializeDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public List<MaterializeDataType> getTypes(MaterializeDataType returnType) {
            return Arrays.asList(returnType);
        }

        public boolean supportsReturnType(MaterializeDataType returnType) {
            return Arrays.asList(supportedReturnTypes).stream().anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public static List<MaterializeAggregateFunction> getAggregates(MaterializeDataType type) {
            return Arrays.asList(values()).stream().filter(p -> p.supportsReturnType(type))
                    .collect(Collectors.toList());
        }

        public MaterializeDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(MaterializeDataType.getRandomType());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }

    }

    public MaterializeAggregate(List<MaterializeExpression> args, MaterializeAggregateFunction func) {
        super(func, args);
    }

}
