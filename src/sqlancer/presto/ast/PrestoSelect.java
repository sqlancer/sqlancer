package sqlancer.presto.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;

public class PrestoSelect extends SelectBase<PrestoExpression>
        implements PrestoExpression, Select<PrestoJoin, PrestoExpression, PrestoTable, PrestoColumn> {

    private boolean isDistinct;

    @Override
    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public void setJoinClauses(List<JoinBase<PrestoExpression>> joinStatements) {
        List<PrestoExpression> expressions = joinStatements.stream().map(e -> (PrestoExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<JoinBase<PrestoExpression>> getJoinClauses() {
        return getJoinList().stream().map(e -> (JoinBase<PrestoExpression>) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return PrestoToStringVisitor.asString(this);
    }
}
