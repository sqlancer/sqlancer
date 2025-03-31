package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresOrderByTerm;
import sqlancer.postgres.ast.PostgresWindowFunction;
import sqlancer.postgres.ast.PostgresWindowFunction.WindowFrame;
import sqlancer.postgres.ast.PostgresWindowFunction.WindowSpecification;

public final class PostgresWindowFunctionGenerator {

    private static final List<String> WINDOW_FUNCTIONS = Arrays.asList("row_number", "rank", "dense_rank",
            "percent_rank", "cume_dist", "ntile", "lag", "lead", "first_value", "last_value", "nth_value");

    private PostgresWindowFunctionGenerator() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    public static PostgresWindowFunction generateWindowFunction(PostgresGlobalState globalState,
            List<PostgresExpression> availableExpr) {

        String functionName = selectRandomWindowFunction();
        List<PostgresExpression> arguments = generateFunctionArguments(functionName, globalState, availableExpr);
        WindowSpecification windowSpec = generateWindowSpecification(globalState, availableExpr);
        PostgresDataType returnType = determineReturnType(functionName);

        return new PostgresWindowFunction(functionName, arguments, windowSpec, returnType);
    }

    private static String selectRandomWindowFunction() {
        return Randomly.fromList(WINDOW_FUNCTIONS);
    }

    private static List<PostgresExpression> generateFunctionArguments(String functionName,
            PostgresGlobalState globalState, List<PostgresExpression> availableExpr) {
        List<PostgresExpression> arguments = new ArrayList<>();

        switch (functionName) {
        case "ntile":
            arguments
                    .add(PostgresExpressionGenerator.generateConstant(globalState.getRandomly(), PostgresDataType.INT));
            break;
        case "lag":
        case "lead":
        case "nth_value":
            arguments.add(Randomly.fromList(availableExpr));
            if (Randomly.getBoolean()) {
                arguments.add(
                        PostgresExpressionGenerator.generateConstant(globalState.getRandomly(), PostgresDataType.INT));
            }
            break;
        case "first_value":
        case "last_value":
            arguments.add(Randomly.fromList(availableExpr));
            break;
        default:
            // No arguments needed for other window functions
            break;
        }

        return arguments;
    }

    private static WindowSpecification generateWindowSpecification(PostgresGlobalState globalState,
            List<PostgresExpression> availableExpr) {
        List<PostgresExpression> partitionBy = generatePartitionByClause(availableExpr);
        PostgresExpressionGenerator exprGen = new PostgresExpressionGenerator(globalState);
        List<PostgresExpression> orderBys = exprGen.generateOrderBys();
        List<PostgresOrderByTerm> orderByTerms = new ArrayList<>();
        for (PostgresExpression expr : orderBys) {
            orderByTerms.add(new PostgresOrderByTerm(expr, Randomly.getBoolean()));
        }

        WindowFrame frame = generateWindowFrame(globalState);
        return new WindowSpecification(partitionBy, orderByTerms, frame);
    }

    private static List<PostgresExpression> generatePartitionByClause(List<PostgresExpression> availableExpr) {
        List<PostgresExpression> partitionBy = new ArrayList<>();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            int count = Randomly.smallNumber();
            for (int i = 0; i < count; i++) {
                partitionBy.add(Randomly.fromList(availableExpr));
            }
        }
        return partitionBy;
    }

    private static WindowFrame generateWindowFrame(PostgresGlobalState globalState) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            WindowFrame.FrameType frameType = Randomly.fromOptions(WindowFrame.FrameType.values());
            PostgresExpression startExpr = generateFrameBound(globalState);
            PostgresExpression endExpr = generateFrameBound(globalState);
            return new WindowFrame(frameType, startExpr, endExpr);
        }
        return null;
    }

    private static PostgresExpression generateFrameBound(PostgresGlobalState globalState) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return generateCurrentRowBound();
        } else {
            return generateOffsetBound(globalState);
        }
    }

    private static PostgresExpression generateCurrentRowBound() {
        return PostgresConstant.createIntConstant(0);
    }

    private static PostgresExpression generateOffsetBound(PostgresGlobalState globalState) {
        return PostgresConstant.createIntConstant(globalState.getRandomly().getInteger());
    }

    private static PostgresDataType determineReturnType(String functionName) {
        switch (functionName) {
        case "percent_rank":
        case "cume_dist":
            return PostgresDataType.FLOAT;
        default:
            return PostgresDataType.INT;
        }
    }
}
