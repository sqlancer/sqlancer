package sqlancer.common.oracle;

import sqlancer.GlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractRowValue;

public abstract class PivotedQuerySynthesisBase<S extends GlobalState<?, ?>, R extends AbstractRowValue<?, ?, ?>, E>
        implements TestOracle {

    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final S globalState;
    protected R pivotRow;

    public PivotedQuerySynthesisBase(S globalState) {
        this.globalState = globalState;
    }

}
