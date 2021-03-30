package sqlancer.mongodb.ast;

import java.io.Serializable;

import org.bson.BsonDateTime;
import org.bson.BsonTimestamp;
import org.bson.Document;

import sqlancer.common.ast.newast.Node;

public abstract class MongoDBConstant implements Node<MongoDBExpression> {
    private MongoDBConstant() {
    }

    public abstract void setValueInDocument(Document document, String key);

    public abstract String getLogValue();

    public abstract Object getValue();

    public abstract Serializable getSerializedValue();

    public static class MongoDBNullConstant extends MongoDBConstant {

        @Override
        public void setValueInDocument(Document document, String key) {
            document.append(key, null);
        }

        @Override
        public String getLogValue() {
            return "null";
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public Serializable getSerializedValue() {
            return null;
        }
    }

    public static Node<MongoDBExpression> createNullConstant() {
        return new MongoDBNullConstant();
    }

    public static class MongoDBIntegerConstant extends MongoDBConstant {

        private final int value;

        public MongoDBIntegerConstant(int value) {
            this.value = value;
        }

        @Override
        public void setValueInDocument(Document document, String key) {
            document.append(key, value);
        }

        @Override
        public String getLogValue() {
            return "NumberInt(" + value + ")";
        }

        @Override
        public Integer getValue() {
            return value;
        }

        @Override
        public Serializable getSerializedValue() {
            return value;
        }
    }

    public static Node<MongoDBExpression> createIntegerConstant(int value) {
        return new MongoDBIntegerConstant(value);
    }

    public static class MongoDBStringConstant extends MongoDBConstant {

        private final String value;

        public MongoDBStringConstant(String value) {
            this.value = value;
        }

        public String getStringValue() {
            return value;
        }

        @Override
        public void setValueInDocument(Document document, String key) {
            document.append(key, value);
        }

        @Override
        public String getLogValue() {
            return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public Serializable getSerializedValue() {
            return value;
        }
    }

    public static Node<MongoDBExpression> createStringConstant(String value) {
        return new MongoDBStringConstant(value);
    }

    public static class MongoDBBooleanConstant extends MongoDBConstant {

        private final boolean value;

        public MongoDBBooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public void setValueInDocument(Document document, String key) {
            document.append(key, value);
        }

        @Override
        public String getLogValue() {
            return String.valueOf(value);
        }

        @Override
        public Boolean getValue() {
            return value;
        }

        @Override
        public Serializable getSerializedValue() {
            return value;
        }
    }

    public static Node<MongoDBExpression> createBooleanConstant(boolean value) {
        return new MongoDBBooleanConstant(value);
    }

    public static class MongoDBDoubleConstant extends MongoDBConstant {

        private final double value;

        public MongoDBDoubleConstant(double value) {
            this.value = value;
        }

        @Override
        public void setValueInDocument(Document document, String key) {
            document.append(key, value);
        }

        @Override
        public String getLogValue() {
            return String.valueOf(value);
        }

        @Override
        public Double getValue() {
            return value;
        }

        @Override
        public Serializable getSerializedValue() {
            return value;
        }
    }

    public static Node<MongoDBExpression> createDoubleConstant(double value) {
        return new MongoDBDoubleConstant(value);
    }

    public static class MongoDBDateTimeConstant extends MongoDBConstant {

        private final BsonDateTime value;

        public MongoDBDateTimeConstant(long val) {
            this.value = new BsonDateTime(val);
        }

        @Override
        public void setValueInDocument(Document document, String key) {
            document.append(key, value);
        }

        @Override
        public String getLogValue() {
            return "new Date(" + value.getValue() + ")";
        }

        @Override
        public BsonDateTime getValue() {
            return value;
        }

        @Override
        public Serializable getSerializedValue() {
            return value.getValue();
        }
    }

    public static Node<MongoDBExpression> createDateTimeConstant(long value) {
        return new MongoDBDateTimeConstant(value);
    }

    public static class MongoDBTimestampConstant extends MongoDBConstant {

        private final BsonTimestamp value;

        public MongoDBTimestampConstant(long value) {
            this.value = new BsonTimestamp(value);
        }

        @Override
        public void setValueInDocument(Document document, String key) {
            document.append(key, value);
        }

        @Override
        public String getLogValue() {
            return "Timestamp(" + value.getValue() + ",1)";
        }

        @Override
        public BsonTimestamp getValue() {
            return value;
        }

        @Override
        public Serializable getSerializedValue() {
            return value.getValue();
        }
    }

    public static Node<MongoDBExpression> createTimestampConstant(long value) {
        return new MongoDBTimestampConstant(value);
    }

}
