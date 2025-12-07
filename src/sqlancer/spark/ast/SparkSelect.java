package sqlancer.spark.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.spark.SparkSchema.SparkColumn;
import sqlancer.spark.SparkSchema.SparkTable;
import sqlancer.spark.SparkToStringVisitor;

public class SparkSelect extends SelectBase<SparkExpression>
        implements Select<SparkJoin, SparkExpression, SparkTable, SparkColumn>, SparkExpression {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public void setJoinClauses(List<SparkJoin> joinStatements) {
        List<SparkExpression> expressions = joinStatements.stream().map(e -> (SparkExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<SparkJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (SparkJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return SparkToStringVisitor.asString(this);
    }

}