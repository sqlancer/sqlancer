package sqlancer.presto.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;

public class PrestoSelect extends SelectBase<PrestoExpression>
        implements PrestoExpression, Select<PrestoJoin, PrestoExpression, PrestoTable, PrestoColumn> {

    private boolean isDistinct;

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public void setJoinClauses(List<PrestoJoin> joinStatements) {
        List<PrestoExpression> expressions = joinStatements.stream().map(e -> (PrestoExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<PrestoJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (PrestoJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return PrestoToStringVisitor.asString(this);
    }
}
