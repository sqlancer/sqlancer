package sqlancer.duckdb.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBSelect extends SelectBase<DuckDBExpression>
        implements Select<DuckDBJoin, DuckDBExpression, DuckDBTable, DuckDBColumn>, DuckDBExpression {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public void setJoinClauses(List<JoinBase<DuckDBExpression>> joinStatements) {
        List<DuckDBExpression> expressions = joinStatements.stream().map(e -> (DuckDBExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<JoinBase<DuckDBExpression>> getJoinClauses() {
        return getJoinList().stream().map(e -> (JoinBase<DuckDBExpression>) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return DuckDBToStringVisitor.asString(this);
    }
}
