package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;
import sqlancer.yugabyte.ysql.ast.YSQLJSONBOperation.YSQLJSONBOperator;

public class YSQLJSONBOperation extends BinaryOperatorNode<YSQLExpression, YSQLJSONBOperator>
        implements YSQLExpression {

    public YSQLJSONBOperation(YSQLExpression left, YSQLExpression right, YSQLJSONBOperator op) {
        super(left, right, op);
    }

    @Override
    public YSQLDataType getExpressionType() {
        switch (getOp()) {
        case CONTAINS:
        case CONTAINED_BY:
        case KEY_EXISTS:
        case ANY_KEY_EXISTS:
        case ALL_KEYS_EXIST:
            return YSQLDataType.BOOLEAN;
        case GET_AS_TEXT:
        case GET_PATH_AS_TEXT:
            return YSQLDataType.TEXT;
        case GET_AS_JSON:
        case GET_PATH_AS_JSON:
        case DELETE_KEY:
        case DELETE_PATH:
        case DELETE_KEYS:
            return YSQLDataType.JSONB;
        default:
            return YSQLDataType.JSONB;
        }
    }

    public enum YSQLJSONBOperator implements Operator {
        // Containment operators
        CONTAINS("@>") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.JSONB};
            }
        },
        CONTAINED_BY("<@") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.JSONB};
            }
        },
        
        // Key existence operators
        KEY_EXISTS("?") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT};
            }
        },
        ANY_KEY_EXISTS("?|") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT_ARRAY};
            }
        },
        ALL_KEYS_EXIST("?&") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT_ARRAY};
            }
        },
        
        // Path operators
        GET_AS_JSON("->") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT};
            }
        },
        GET_AS_TEXT("->>") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT};
            }
        },
        GET_PATH_AS_JSON("#>") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT_ARRAY};
            }
        },
        GET_PATH_AS_TEXT("#>>") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT_ARRAY};
            }
        },
        
        // Deletion operators
        DELETE_KEY("-") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT};
            }
        },
        DELETE_PATH("#-") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT_ARRAY};
            }
        },
        DELETE_KEYS("-") {
            @Override
            public YSQLDataType[] getInputDataTypes() {
                return new YSQLDataType[]{YSQLDataType.JSONB, YSQLDataType.TEXT_ARRAY};
            }
        };

        private final String textRepresentation;

        YSQLJSONBOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static YSQLJSONBOperator getRandom() {
            return Randomly.fromOptions(YSQLJSONBOperator.values());
        }
        
        public static YSQLJSONBOperator getRandomBooleanOperator() {
            return Randomly.fromOptions(CONTAINS, CONTAINED_BY, KEY_EXISTS, ANY_KEY_EXISTS, ALL_KEYS_EXIST);
        }
        
        public static YSQLJSONBOperator getRandomExtractionOperator() {
            return Randomly.fromOptions(GET_AS_JSON, GET_AS_TEXT, GET_PATH_AS_JSON, GET_PATH_AS_TEXT);
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
        
        public abstract YSQLDataType[] getInputDataTypes();
    }
}