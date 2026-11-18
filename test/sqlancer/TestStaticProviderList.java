package sqlancer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

public class TestStaticProviderList {

    /**
     * The statically loaded providers are the fallback for https://github.com/sqlancer/sqlancer/issues/799. They must
     * contain exactly the providers that are registered through the @AutoService annotation. Providers that are defined
     * by tests (e.g., for the reducer tests) are not part of SQLancer and are ignored.
     */
    @Test
    void testStaticProvidersMatchRegisteredProviders() {
        Object sqlancerLocation = Main.class.getProtectionDomain().getCodeSource().getLocation();
        Set<String> registered = new TreeSet<>();
        @SuppressWarnings("rawtypes")
        ServiceLoader<DatabaseProvider> loader = ServiceLoader.load(DatabaseProvider.class);
        for (DatabaseProvider<?, ?, ?> provider : loader) {
            Object providerLocation = provider.getClass().getProtectionDomain().getCodeSource().getLocation();
            if (sqlancerLocation.equals(providerLocation)) {
                registered.add(provider.getClass().getName());
            }
        }
        Set<String> statics = new TreeSet<>();
        for (DatabaseProvider<?, ?, ?> provider : Main.getStaticallyLoadedProviders()) {
            statics.add(provider.getClass().getName());
        }
        assertEquals(registered, statics);
    }

}
