package sqlancer.common.oracle;

import sqlancer.GlobalState;
import sqlancer.common.gen.ExpressionGenerator;

public abstract class RemoveReduceOracleBase<E, S extends GlobalState<?, ?, ?>> implements TestOracle {

    protected E predicate;

    protected final S state;

    protected RemoveReduceOracleBase(S state) {
        this.state = state;
    }

    protected void initializeRemoveReduceOracle() {
        ExpressionGenerator<E> gen = getGen();
        if (gen == null) {
            throw new IllegalStateException();
        }
        predicate = gen.generatePredicate();
        if (predicate == null) {
            throw new IllegalStateException();
        }
    }

    protected abstract ExpressionGenerator<E> getGen();

}
