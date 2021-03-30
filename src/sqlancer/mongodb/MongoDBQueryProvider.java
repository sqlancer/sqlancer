package sqlancer.mongodb;

@FunctionalInterface
public interface MongoDBQueryProvider<S> {
    MongoDBQueryAdapter getQuery(S globalState) throws Exception;
}
