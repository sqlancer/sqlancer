package sqlancer;

import sqlancer.FoundBugException.Reproducer;

public interface Reducer<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> {

    void reduce(G state, Reproducer reproducer, G newGlobalState) throws Exception;

}
