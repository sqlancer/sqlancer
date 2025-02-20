package sqlancer.databend.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.DatabendToStringVisitor;

public class DatabendSelect extends SelectBase<DatabendExpression>
        implements DatabendExpression, Select<DatabendJoin, DatabendExpression, DatabendTable, DatabendColumn> {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public void setJoinClauses(List<JoinBase<DatabendExpression>> joinStatements) {
        List<DatabendExpression> expressions = joinStatements.stream().map(e -> (DatabendExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<JoinBase<DatabendExpression>> getJoinClauses() {
        return getJoinList().stream()
                .map(e -> (JoinBase<DatabendExpression>) e)
                .collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return DatabendToStringVisitor.asString(this);
    }
}
