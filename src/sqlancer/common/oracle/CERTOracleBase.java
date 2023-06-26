package sqlancer.common.oracle;

import java.util.Arrays;
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

        public static Mutator getRandomExcept(Mutator... exclude) {
            Mutator[] values = Arrays.stream(values()).filter(m -> !Arrays.asList(exclude).contains(m))
                    .toArray(Mutator[]::new);
            return Randomly.fromOptions(values);
        }
    }

    protected CERTOracleBase(S state) {
        this.state = state;
        this.errors = new ExpectedErrors();
    }

    protected boolean mutate(Mutator... exclude) {
        Mutator m = Mutator.getRandomExcept(exclude);
        switch (m) {
        case JOIN:
            return mutateJoin();
        case DISTINCT:
            return mutateDistinct();
        case WHERE:
            return mutateWhere();
        case GROUPBY:
            return mutateGroupBy();
        case HAVING:
            return mutateHaving();
        case AND:
            return mutateAnd();
        case OR:
            return mutateOr();
        case LIMIT:
            return mutateLimit();
        default:
            throw new AssertionError(m);
        }
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
