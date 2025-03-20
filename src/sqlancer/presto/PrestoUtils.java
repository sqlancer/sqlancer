package sqlancer.presto;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;

public final class PrestoUtils {

    private PrestoUtils() {
    }

    public static PrestoPair<List<PrestoSchema.PrestoCompositeDataType>, PrestoSchema.PrestoCompositeDataType> prepareVariadicArgumentTypes(
            PrestoSchema.PrestoDataType dataType, PrestoSchema.PrestoCompositeDataType savedArrayType) {

        List<PrestoSchema.PrestoCompositeDataType> typeList = new ArrayList<>();
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
            typeList.add(type);
        }

        return new PrestoPair<>(typeList, currentArrayType);
    }
}
