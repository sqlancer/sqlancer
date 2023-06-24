package sqlancer.common.oracle;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.ExpectedErrors;

public abstract class CERTOracleBase<S extends SQLGlobalState<?, ?>> implements TestOracle<S> {

    protected final S state;
    protected final ExpectedErrors errors;
    protected List<String> queryPlan1Sequences;
    protected List<String> queryPlan2Sequences;

    protected enum Mutator {
        JOIN, DISTINCT, WHERE, GROUPBY, HAVING, AND, OR, LIMIT;

        public static Mutator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    protected CERTOracleBase(S state) {
        this.state = state;
        this.errors = new ExpectedErrors();
    }

    protected boolean mutateJoin() {
        throw new UnsupportedOperationException();
    }

    protected boolean mutateDistinct() {
        throw new UnsupportedOperationException();
    }

    protected boolean mutateWhere() {
        throw new UnsupportedOperationException();
    }

    protected boolean mutateGroupBy() {
        throw new UnsupportedOperationException();
    }

    protected boolean mutateHaving() {
        throw new UnsupportedOperationException();
    }

    protected boolean mutateAnd() {
        throw new UnsupportedOperationException();
    }

    protected boolean mutateOr() {
        throw new UnsupportedOperationException();
    }

    protected boolean mutateLimit() {
        throw new UnsupportedOperationException();
    }

}
