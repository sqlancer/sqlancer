package sqlancer;

public class CReduceReducer<G extends GlobalState<?, ?, ?>> implements Reducer<G> {

    public CReduceReducer() {
    }

    @Override
    public void reduce(G state, Reproducer<G> reproducer, G newGlobalState) throws Exception {

        System.out.println("CReduceReducer is running...");

        if (state == null || reproducer == null || newGlobalState == null) {
            throw new IllegalArgumentException("Invalid state or reproducer");
        }

    }
}