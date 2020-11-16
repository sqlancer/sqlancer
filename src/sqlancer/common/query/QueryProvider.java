package sqlancer.common.query;

@FunctionalInterface
public interface QueryProvider<S> {
    Query getQuery(S globalState) throws Exception;
}
