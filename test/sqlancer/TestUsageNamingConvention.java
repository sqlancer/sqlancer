package sqlancer;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.Builder;
import com.beust.jcommander.Parameters;

public class TestUsageNamingConvention {

    @Test
    void testNonEmptyDescription() {
        List<DatabaseProvider<?, ?, ?>> providers = Main.getDBMSProviders();
        MainOptions options = new MainOptions();
        Builder commandBuilder = JCommander.newBuilder().addObject(options);
        for (DatabaseProvider<?, ?, ?> provider : providers) {
            String name = provider.getDBMSName();
            if (!name.toLowerCase().equals(name)) {
                throw new AssertionError(name + " should be in lowercase!");
            }
            commandBuilder.addCommand(provider.getDBMSName(), provider.getOptionClass());
            Parameters param = provider.getOptionClass().getAnnotation(Parameters.class);
            assertNotEquals(null, param, provider.getOptionClass().toString());
            String databaseDescription = param.commandDescription();
            assertNotEquals(null, databaseDescription, "description cannot be empty " + provider.getOptionClass());
            assertNotEquals("", databaseDescription, "description cannot be empty " + provider.getOptionClass());
        }
    }

}
