package sqlancer.simple.statement;

import java.util.List;

import sqlancer.simple.clause.Clause;
import sqlancer.simple.clause.Fetch;
import sqlancer.simple.clause.Where;
import sqlancer.simple.gen.Generator;

public class SimpleSelect implements Select {
    Clause fetchClause;
    Clause fromClause;
    Clause joinClause;
    Clause whereClause;

    public SimpleSelect(Clause fetchClause, Clause fromClause, Clause joinClause, Clause whereClause) {
        this.fetchClause = fetchClause;
        this.fromClause = fromClause;
        this.joinClause = joinClause;
        this.whereClause = whereClause;
    }

    @Override
    public void setClause(Clause clause) {
        if (clause instanceof Fetch) {
            this.fetchClause = clause;
            return;
        }
        if (clause instanceof Where) {
            this.whereClause = clause;
            return;
        }
        throw new IllegalArgumentException("Clause " + clause.getClass() + " not supported by statement");
    }

    @Override
    public String print() {
        return "SELECT " + fetchClause.print() + " FROM " + fromClause.print() + " " + joinClause.print() + " "
                + whereClause.print();
    }

    public static class Builder {
        List<Class<? extends Clause>> legalFromClauses;
        List<Class<? extends Clause>> legalJoinClauses;
        List<Class<? extends Clause>> legalFetchClauses;
        List<Class<? extends Clause>> legalWhereClauses;

        public Builder(List<Class<? extends Clause>> legalFetchClauses, List<Class<? extends Clause>> legalFromClauses,
                List<Class<? extends Clause>> legalJoinClauses, List<Class<? extends Clause>> legalWhereClauses) {
            this.legalFetchClauses = legalFetchClauses;
            this.legalFromClauses = legalFromClauses;
            this.legalJoinClauses = legalJoinClauses;
            this.legalWhereClauses = legalWhereClauses;
        }

        public SimpleSelect build(Generator gen) {
            Clause fromClause = gen.generateClauseOfAny(legalFromClauses);
            Clause joinClause = gen.generateClauseOfAny(legalJoinClauses);
            Clause fetchClause = gen.generateClauseOfAny(legalFetchClauses);
            Clause whereClause = gen.generateClauseOfAny(legalWhereClauses);

            return new SimpleSelect(fetchClause, fromClause, joinClause, whereClause);
        }
    }
}
