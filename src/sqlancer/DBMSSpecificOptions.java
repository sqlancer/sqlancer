package sqlancer;

import java.util.List;

public interface DBMSSpecificOptions<F extends OracleFactory<? extends GlobalState<?, ?, ?>>> {

    List<F> getTestOracleFactory();

}
