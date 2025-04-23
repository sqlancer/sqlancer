package sqlancer.hive.ast;

import java.util.List;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.hive.HiveToStringVisitor;
import sqlancer.hive.HiveSchema.HiveColumn;
import sqlancer.hive.HiveSchema.HiveTable;

public class HiveSelect extends SelectBase<HiveExpression>
        implements Select<HiveJoin, HiveExpression, HiveTable, HiveColumn>, HiveExpression {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public void setJoinClauses(List<HiveJoin> joinStatements) {
        List<HiveExpression> expressions = joinStatements.stream().map(e -> (HiveExpression) e)
                .collect(java.util.stream.Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<HiveJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (HiveJoin) e).collect(java.util.stream.Collectors.toList());
    }

    @Override
    public String asString() {
        return HiveToStringVisitor.asString(this);
    }

}
