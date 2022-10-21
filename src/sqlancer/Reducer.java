package sqlancer;

import sqlancer.FoundBugException.Reproducer;

public interface Reducer<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> {

    public void reduce(G state, Reproducer reproducer, G newGlobalState) throws Exception;

}
