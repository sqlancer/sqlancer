package sqlancer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.Builder;
import com.beust.jcommander.ParameterDescription;

/**
 * Check that consistent option names are used (those that are displayed when launcing SQLancer without options).
 */
public class TestParameterFormat {

    private final static String OPTION_REGEX = "(-)?-[a-z0-9-]*";

    @Test
    public void testOptionFormat() throws Exception {
        List<DatabaseProvider<?, ?, ?>> providers = Main.getDBMSProviders();
        MainOptions options = new MainOptions();
        Builder commandBuilder = JCommander.newBuilder().addObject(options);
        List<ParameterDescription> parameterDescriptions = new ArrayList<>();
        for (int i = 0; i < providers.size(); i++) {
            commandBuilder = commandBuilder.addCommand(String.format("db%d", i),
                    providers.get(i).getOptionClass().getConstructor().newInstance());
        }
        JCommander jc = commandBuilder.programName("SQLancer").build();
        jc.parse(new String[0]);

        parameterDescriptions.addAll(jc.getParameters());
        for (String commandName : jc.getCommands().keySet()) {
            JCommander command = jc.getCommands().get(commandName);
            parameterDescriptions.addAll(command.getParameters());
        }
        for (ParameterDescription parameter : parameterDescriptions) {
            String[] names = parameter.getNames().split(", ");
            for (String name : names) {
                assertTrue(Pattern.matches(OPTION_REGEX, name), name);
            }
        }
    }

}
