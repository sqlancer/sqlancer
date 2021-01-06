package sqlancer.mongodb.gen;

import org.bson.Document;

import sqlancer.Randomly;
import sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState;
import sqlancer.mongodb.MongoDBSchema.MongoDBDataType;
import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBConstant.MongoDBBooleanConstant;
import sqlancer.mongodb.ast.MongoDBConstant.MongoDBDateTimeConstant;
import sqlancer.mongodb.ast.MongoDBConstant.MongoDBDoubleConstant;
import sqlancer.mongodb.ast.MongoDBConstant.MongoDBIntegerConstant;
import sqlancer.mongodb.ast.MongoDBConstant.MongoDBNullConstant;
import sqlancer.mongodb.ast.MongoDBConstant.MongoDBStringConstant;
import sqlancer.mongodb.ast.MongoDBConstant.MongoDBTimestampConstant;

public class MongoDBConstantGenerator {
    private final MongoDBGlobalState globalState;

    public MongoDBConstantGenerator(MongoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public void addRandomConstant(Document document, String key) {
        MongoDBDataType type = MongoDBDataType.getRandom();
        addRandomConstantWithType(document, key, type);
    }

    public void addRandomConstantWithType(Document document, String key, MongoDBDataType option) {
        MongoDBConstant constant;
        if (globalState.getDmbsSpecificOptions().testNullInserts && Randomly.getBooleanWithSmallProbability()) {
            constant = new MongoDBNullConstant();
            constant.setValueInDocument(document, key);
            return;
        }
        switch (option) {
        case DATE_TIME:
            constant = new MongoDBDateTimeConstant(globalState.getRandomly().getInteger());
            constant.setValueInDocument(document, key);
            return;

        case BOOLEAN:
            constant = new MongoDBBooleanConstant(Randomly.getBoolean());
            constant.setValueInDocument(document, key);
            return;
        case DOUBLE:
            constant = new MongoDBDoubleConstant(globalState.getRandomly().getDouble());
            constant.setValueInDocument(document, key);
            return;
        case STRING:
            constant = new MongoDBStringConstant(globalState.getRandomly().getString());
            constant.setValueInDocument(document, key);
            return;
        case INTEGER:
            constant = new MongoDBIntegerConstant((int) globalState.getRandomly().getInteger());
            constant.setValueInDocument(document, key);
            return;
        case TIMESTAMP:
            constant = new MongoDBTimestampConstant(globalState.getRandomly().getInteger());
            constant.setValueInDocument(document, key);
            return;
        default:
            throw new AssertionError(option);
        }
    }
}
