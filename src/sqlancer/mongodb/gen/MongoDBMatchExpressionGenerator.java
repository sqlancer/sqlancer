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
import sqlancer.mongodb.ast.MongoDBRegexNode;
import sqlancer.mongodb.ast.MongoDBUnaryLogicalOperatorNode;
import sqlancer.mongodb.ast.MongoDBUnsupportedPredicate;
import sqlancer.mongodb.test.MongoDBColumnTestReference;
import sqlancer.mongodb.visitor.MongoDBNegateVisitor;

public class MongoDBMatchExpressionGenerator
        extends UntypedExpressionGenerator<Node<MongoDBExpression>, MongoDBColumnTestReference> {

    private final MongoDBGlobalState globalState;

    private enum LeafExpression {
        BINARY_COMPARISON, REGEX
    }

    private enum NonLeafExpression {
        BINARY_LOGICAL, UNARY_LOGICAL
    }

    public MongoDBMatchExpressionGenerator(MongoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public Node<MongoDBExpression> generateLeafNode() {
        List<LeafExpression> possibleOptions = new ArrayList<>(Arrays.asList(LeafExpression.values()));
        if (!globalState.getDmbsSpecificOptions().testWithRegex) {
            possibleOptions.remove(LeafExpression.REGEX);
        }
        LeafExpression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case BINARY_COMPARISON:
            MongoDBBinaryComparisonOperator operator = MongoDBBinaryComparisonOperator.getRandom();
            MongoDBColumnTestReference reference = (MongoDBColumnTestReference) generateColumn();

            return new MongoDBBinaryComparisonNode(reference,
                    generateConstant(reference.getColumnReference().getType()), operator);
        case REGEX:
            return new MongoDBRegexNode(generateColumn(),
                    new MongoDBConstantGenerator(globalState).generateConstantWithType(MongoDBDataType.STRING),
                    getRandomizedRegexOptions());
        default:
            throw new AssertionError();
        }
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
        MongoDBDataType type = MongoDBDataType.getRandom(globalState);
        MongoDBConstantGenerator generator = new MongoDBConstantGenerator(globalState);
        if (Randomly.getBooleanWithSmallProbability()) {
            return MongoDBConstant.createNullConstant();
        }
        return generator.generateConstantWithType(type);
    }

    public Node<MongoDBExpression> generateConstant(MongoDBDataType type) {
        MongoDBConstantGenerator generator = new MongoDBConstantGenerator(globalState);
        if (Randomly.getBooleanWithSmallProbability() && !globalState.getDmbsSpecificOptions().nullSafety) {
            return MongoDBConstant.createNullConstant();
        }
        return generator.generateConstantWithType(type);
    }

    private String getRandomizedRegexOptions() {
        List<String> s = Randomly.subset("i", "m", "x", "s");
        return String.join("", s);
    }

    @Override
    protected Node<MongoDBExpression> generateColumn() {
        return Randomly.fromList(columns);
    }

    @Override
    public Node<MongoDBExpression> generatePredicate() {
        Node<MongoDBExpression> result = super.generatePredicate();
        return MongoDBNegateVisitor.cleanNegations(result);
    }

    @Override
    public Node<MongoDBExpression> negatePredicate(Node<MongoDBExpression> predicate) {
        Node<MongoDBExpression> result = new MongoDBUnaryLogicalOperatorNode(predicate,
                MongoDBUnaryLogicalOperator.NOT);
        return MongoDBNegateVisitor.cleanNegations(result);
    }

    @Override
    public Node<MongoDBExpression> isNull(Node<MongoDBExpression> expr) {
        return new MongoDBUnsupportedPredicate<>();
    }

    public enum MongoDBUnaryLogicalOperator implements Operator {
        NOT {
            @Override
            public Bson applyOperator(Bson inner) {
                return Filters.not(inner);
            }

            @Override
            public String getTextRepresentation() {
                return "$not";
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

    public enum MongoDBRegexOperator implements Operator {
        REGEX {
            @Override
            public Bson applyOperator(String columnName, MongoDBConstant.MongoDBStringConstant regex, String options) {
                return Filters.regex(columnName, regex.getStringValue(), options);
            }

            @Override
            public String getTextRepresentation() {
                return "$regex";
            }
        };

        public abstract Bson applyOperator(String columnName, MongoDBConstant.MongoDBStringConstant regex,
                String options);
    }
}
