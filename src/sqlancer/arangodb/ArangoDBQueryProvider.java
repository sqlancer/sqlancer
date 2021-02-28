package sqlancer.arangodb;

@FunctionalInterface
public interface ArangoDBQueryProvider<S> {
    ArangoDBQueryAdapter getQuery(S globalState) throws Exception;
}
