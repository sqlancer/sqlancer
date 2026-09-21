package sqlancer.simple.gen;

import java.util.List;

import sqlancer.simple.clause.Clause;

public interface Generator {

    Clause generateClauseOfAny(List<Class<? extends Clause>> clauses);

    <T> T generateResponse(Signal signal);

    void reset();
}
