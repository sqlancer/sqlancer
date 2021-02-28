package sqlancer.arangodb.query;

import com.arangodb.entity.BaseDocument;

import sqlancer.Randomly;
import sqlancer.arangodb.ArangoDBProvider;
import sqlancer.arangodb.ArangoDBSchema;
import sqlancer.arangodb.ast.ArangoDBConstant;

public class ArangoDBConstantGenerator {
    private final ArangoDBProvider.ArangoDBGlobalState globalState;

    public ArangoDBConstantGenerator(ArangoDBProvider.ArangoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public void addRandomConstant(BaseDocument document, String key) {
        ArangoDBSchema.ArangoDBDataType type = ArangoDBSchema.ArangoDBDataType.getRandom();
        addRandomConstantWithType(document, key, type);
    }

    public void addRandomConstantWithType(BaseDocument document, String key, ArangoDBSchema.ArangoDBDataType dataType) {
        ArangoDBConstant constant;
        switch (dataType) {
        case STRING:
            constant = new ArangoDBConstant.ArangoDBStringConstant(globalState.getRandomly().getString());
            constant.setValueInDocument(document, key);
            return;
        case DOUBLE:
            constant = new ArangoDBConstant.ArangoDBDoubleConstant(globalState.getRandomly().getDouble());
            constant.setValueInDocument(document, key);
            return;
        case BOOLEAN:
            constant = new ArangoDBConstant.ArangoDBBooleanConstant(Randomly.getBoolean());
            constant.setValueInDocument(document, key);
            return;
        case INTEGER:
            constant = new ArangoDBConstant.ArangoDBIntegerConstant((int) globalState.getRandomly().getInteger());
            constant.setValueInDocument(document, key);
            return;
        default:
            throw new AssertionError(dataType);
        }

    }
}
