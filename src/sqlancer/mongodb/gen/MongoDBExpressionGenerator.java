package sqlancer.mongodb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState;
import sqlancer.mongodb.MongoDBSchema.MongoDBDataType;
import sqlancer.mongodb.ast.MongoDBBinaryComparisonNode;
import sqlancer.mongodb.ast.MongoDBBinaryLogicalNode;
import sqlancer.mongodb.ast.MongoDBConstant;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBUnaryLogicalOperatorNode;
import sqlancer.mongodb.ast.MongoDBUnsupportedPredicate;
import sqlancer.mongodb.test.MongoDBColumnTestReference;

public class MongoDBExpressionGenerator
        extends UntypedExpressionGenerator<Node<MongoDBExpression>, MongoDBColumnTestReference> {

    private final MongoDBGlobalState globalState;

    private enum NonLeafExpression {
        BINARY_LOGICAL, UNARY_LOGICAL
    }

    public MongoDBExpressionGenerator(MongoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public Node<MongoDBExpression> generateLeafNode() {
        MongoDBBinaryComparisonOperator operator = MongoDBBinaryComparisonOperator.getRandom();
        return new MongoDBBinaryComparisonNode(generateColumn(), generateConstant(), operator);
    }

    @Override
    protected Node<MongoDBExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }

        List<NonLeafExpression> possibleOptions = new ArrayList<>(Arrays.asList(NonLeafExpression.values()));
        NonLeafExpression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case BINARY_LOGICAL:
            MongoDBBinaryLogicalOperator binaryOperator = MongoDBBinaryLogicalOperator.getRandom();
            return new MongoDBBinaryLogicalNode(generateExpression(depth + 1), generateExpression(depth + 1),
                    binaryOperator);
        case UNARY_LOGICAL:
            MongoDBUnaryLogicalOperator unaryOperator = MongoDBUnaryLogicalOperator.getRandom();
            return new MongoDBUnaryLogicalOperatorNode(generateExpression(depth + 1), unaryOperator);
        default:
            throw new AssertionError();
        }
    }

    @Override
    public Node<MongoDBExpression> generateConstant() {
        MongoDBDataType type = MongoDBDataType.getRandom();
        MongoDBConstantGenerator generator = new MongoDBConstantGenerator(globalState);
        return generator.generateConstantWithType(type);
    }

    @Override
    protected Node<MongoDBExpression> generateColumn() {
        return Randomly.fromList(columns);
    }

    @Override
    public Node<MongoDBExpression> negatePredicate(Node<MongoDBExpression> predicate) {
        return new MongoDBUnaryLogicalOperatorNode(predicate, MongoDBUnaryLogicalOperator.NOT);
    }

    @Override
    public Node<MongoDBExpression> isNull(Node<MongoDBExpression> expr) {
        return new MongoDBUnsupportedPredicate<>();
    }

    public enum MongoDBUnaryLogicalOperator implements Operator {
        NOT {
            @Override
            public Bson applyOperator(Bson inner) {
                return Filters.nor(inner, Filters.exists("_id", false));
            }

            @Override
            public String getTextRepresentation() {
                return "{$nor: [{ _id: {$exists: false}}, ";
            }
        };

        public abstract Bson applyOperator(Bson inner);

        public static MongoDBUnaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum MongoDBBinaryLogicalOperator implements Operator {
        AND {
            @Override
            public Bson applyOperator(Bson left, Bson right) {
                return Filters.and(left, right);
            }

            @Override
            public String getTextRepresentation() {
                return "$and";
            }
        },
        OR {
            @Override
            public Bson applyOperator(Bson left, Bson right) {
                return Filters.or(left, right);
            }

            @Override
            public String getTextRepresentation() {
                return "$or";
            }
        },
        NOR {
            @Override
            public Bson applyOperator(Bson left, Bson right) {
                return Filters.nor(left, right);
            }

            @Override
            public String getTextRepresentation() {
                return "$nor";
            }
        };

        public abstract Bson applyOperator(Bson left, Bson right);

        public static MongoDBBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum MongoDBBinaryComparisonOperator implements Operator {
        EQUALS {
            @Override
            public Bson applyOperator(String columnName, MongoDBConstant constant) {
                return Filters.eq(columnName, constant.getValue());
            }

            @Override
            public String getTextRepresentation() {
                return "$eq";
            }
        },
        NOT_EQUALS {
            @Override
            public Bson applyOperator(String columnName, MongoDBConstant constant) {
                return Filters.ne(columnName, constant.getValue());
            }

            @Override
            public String getTextRepresentation() {
                return "$ne";
            }
        },
        GREATER {
            @Override
            public Bson applyOperator(String columnName, MongoDBConstant constant) {
                return Filters.gt(columnName, constant.getValue());
            }

            @Override
            public String getTextRepresentation() {
                return "$gt";
            }

        },
        LESS {
            @Override
            public Bson applyOperator(String columnName, MongoDBConstant constant) {
                return Filters.lt(columnName, constant.getValue());
            }

            @Override
            public String getTextRepresentation() {
                return "$lt";
            }

        },
        GREATER_EQUAL {
            @Override
            public Bson applyOperator(String columnName, MongoDBConstant constant) {
                return Filters.gte(columnName, constant.getValue());

            }

            @Override
            public String getTextRepresentation() {
                return "$gte";
            }

        },
        LESS_EQUAL {
            @Override
            public Bson applyOperator(String columnName, MongoDBConstant constant) {
                return Filters.lte(columnName, constant.getValue());
            }

            @Override
            public String getTextRepresentation() {
                return "$lte";
            }
        };

        public abstract Bson applyOperator(String columnName, MongoDBConstant constant);

        public static MongoDBBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }
}
