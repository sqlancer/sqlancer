package sqlancer.cockroachdb.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;

public class CockroachDBSelect extends SelectBase<CockroachDBExpression> implements CockroachDBExpression,
        Select<CockroachDBJoin, CockroachDBExpression, CockroachDBTable, CockroachDBColumn> {

    private boolean isDistinct;

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public void setJoinClauses(List<CockroachDBJoin> joinStatements) {
        List<CockroachDBExpression> expressions = joinStatements.stream().map(e -> (CockroachDBExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<CockroachDBJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (CockroachDBJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return CockroachDBVisitor.asString(this);
    }

}
