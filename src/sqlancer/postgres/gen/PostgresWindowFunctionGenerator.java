package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresOrderByTerm;
import sqlancer.postgres.ast.PostgresWindowFunction;
import sqlancer.postgres.ast.PostgresWindowFunction.WindowFrame;
import sqlancer.postgres.ast.PostgresWindowFunction.WindowSpecification;

public class PostgresWindowFunctionGenerator {
    
    private static final List<String> WINDOW_FUNCTIONS = Arrays.asList(
        "row_number", "rank", "dense_rank", "percent_rank",
        "cume_dist", "ntile", "lag", "lead", "first_value",
        "last_value", "nth_value"
    );

    public static PostgresWindowFunction generateWindowFunction(PostgresGlobalState globalState,
            List<PostgresExpression> availableExpr) {
        
        String functionName = Randomly.fromList(WINDOW_FUNCTIONS);
        List<PostgresExpression> arguments = new ArrayList<>();
        
        // Generate function arguments based on function name
        switch (functionName) {
            case "ntile":
                arguments.add(PostgresExpressionGenerator.generateConstant(globalState.getRandomly()));
                break;
            case "lag":
            case "lead":
            case "nth_value":
                arguments.add(Randomly.fromList(availableExpr));
                if (Randomly.getBoolean()) {
                    arguments.add(PostgresExpressionGenerator.generateConstant(globalState.getRandomly()));
                }
                break;
            case "first_value":
            case "last_value":
                arguments.add(Randomly.fromList(availableExpr));
                break;
            default:
                // No arguments needed for other functions
                break;
        }

        // Generate partition by clause
        List<PostgresExpression> partitionBy = new ArrayList<>();
        if (Randomly.getBoolean()) {
            int count = Randomly.smallNumber();
            for (int i = 0; i < count; i++) {
                partitionBy.add(Randomly.fromList(availableExpr));
            }
        }

        // Generate order by clause
        List<PostgresOrderByTerm> orderBy = new ArrayList<>();
        if (Randomly.getBoolean()) {
            int count = Randomly.smallNumber();
            for (int i = 0; i < count; i++) {
                orderBy.add(new PostgresOrderByTerm(Randomly.fromList(availableExpr),
                    Randomly.getBoolean()));
            }
        }

        // Generate window frame
        WindowFrame frame = null;
        if (Randomly.getBoolean()) {
            WindowFrame.FrameType frameType = Randomly.fromOptions(WindowFrame.FrameType.values());
            PostgresExpression startExpr = PostgresExpressionGenerator.generateConstant(globalState.getRandomly());
            PostgresExpression endExpr = PostgresExpressionGenerator.generateConstant(globalState.getRandomly());
            frame = new WindowFrame(frameType, startExpr, endExpr);
        }

        WindowSpecification windowSpec = new WindowSpecification(partitionBy, orderBy, frame);
        
        // Determine return type based on function
        PostgresDataType returnType;
        switch (functionName) {
            case "percent_rank":
            case "cume_dist":
                returnType = PostgresDataType.FLOAT;
                break;
            default:
                returnType = PostgresDataType.INT;
                break;
        }

        return new PostgresWindowFunction(functionName, arguments, windowSpec, returnType);
    }
}