package sqlancer.cnosdb.query;

@FunctionalInterface
public interface CnosDBQueryProvider<S> {
    CnosDBOtherQuery getQuery(S globalState) throws Exception;
}
