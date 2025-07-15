package sqlancer;

import java.util.Map;

public interface Reproducer<G extends GlobalState<?, ?, ?>> {
    boolean bugStillTriggers(G globalState);

    default Map<String, String> getReproducerData() {
        return null;
    }
}
