package sqlancer.simple.statement;

import sqlancer.simple.clause.Clause;

public interface Select {
    void setClause(Clause clause);

    String print();
}
