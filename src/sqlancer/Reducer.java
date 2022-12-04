package sqlancer;

public interface Reducer<G extends GlobalState<?, ?, ?>> {

    void reduce(G state, Reproducer<G> reproducer, G newGlobalState) throws Exception;

}
