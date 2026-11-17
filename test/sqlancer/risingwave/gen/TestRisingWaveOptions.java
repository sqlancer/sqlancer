package sqlancer.risingwave.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;

import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.RisingWaveOptions;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveConstant;
import sqlancer.risingwave.ast.RisingWaveExpression;

public class TestRisingWaveOptions {
    @Test
    public void sharedExpressionDepthControlsGeneration() {
        for (int depth : List.of(0, 6)) {
            MainOptions main = new MainOptions();
            RisingWaveOptions options = new RisingWaveOptions();
            JCommander.newBuilder().addObject(main).addCommand("risingwave", options).build().parse(
                    "--max-expression-depth", Integer.toString(depth), "risingwave", "--max-relation-depth", "4");
            RisingWaveGlobalState state = new RisingWaveGlobalState();
            state.setMainOptions(main);
            state.setDbmsSpecificOptions(options);
            state.setRandomly(new Randomly(7));
            RisingWaveGenerationContext context = new RisingWaveGenerationContext(state, List.of());
            assertEquals(depth, context.expressionDepth);
            assertEquals(4, context.options.relationDepth);
            RisingWaveExpressionGenerator generator = new RisingWaveExpressionGenerator(context);
            boolean composite = false;
            for (int i = 0; i < 100; i++) {
                RisingWaveExpression expression = generator.expression(RisingWaveType.INT, List.of(), 0, false);
                composite |= !(expression instanceof RisingWaveConstant);
            }
            assertEquals(depth > 0, composite);
        }
    }

    @Test
    public void expressionDepthIsNotDuplicatedInProviderOptions() {
        JCommander provider = JCommander.newBuilder().addObject(new RisingWaveOptions()).build();
        assertTrue(provider.getParameters().stream()
                .noneMatch(parameter -> parameter.getNames().equals("--max-expression-depth")));
        assertEquals(3, new MainOptions().getMaxExpressionDepth());
    }
}
