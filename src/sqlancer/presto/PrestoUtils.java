package sqlancer.presto;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public final class PrestoUtils {
    private PrestoUtils() {
    }

    public static PrestoPair<List<PrestoExpression>, PrestoSchema.PrestoCompositeDataType> generateVariadicArguments(
            PrestoTypedExpressionGenerator generator, PrestoSchema.PrestoDataType dataType,
            PrestoSchema.PrestoCompositeDataType savedArrayType, int depth) {

        List<PrestoExpression> arguments = new ArrayList<>();
        PrestoSchema.PrestoCompositeDataType currentArrayType = savedArrayType;
        // TODO: consider upper
        long no = Randomly.getNotCachedInteger(2, 10);

        for (int i = 0; i < no; i++) {
            PrestoSchema.PrestoCompositeDataType type;

            if (dataType == PrestoSchema.PrestoDataType.ARRAY) {
                if (currentArrayType == null) {
                    currentArrayType = dataType.get();
                }
                type = currentArrayType;
            } else {
                type = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
            }
            arguments.add(generator.generateExpression(type, depth + 1));
        }

        return new PrestoPair<>(arguments, currentArrayType);
    }
}
