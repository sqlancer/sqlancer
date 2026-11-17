package sqlancer.risingwave.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveColumn;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveToStringVisitor;
import sqlancer.risingwave.RisingWaveType;

public class RisingWaveSelect extends SelectBase<RisingWaveExpression> implements RisingWaveExpression,
        Select<RisingWaveJoin, RisingWaveExpression, RisingWaveTable, RisingWaveColumn> {
    private List<List<RisingWaveExpression>> groupingSets = List.of();

    public List<List<RisingWaveExpression>> getGroupingSets() {
        return groupingSets;
    }

    public void setGroupingSets(List<List<RisingWaveExpression>> sets) {
        groupingSets = sets.stream().map(List::copyOf).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void setJoinClauses(List<RisingWaveJoin> joins) {
        setJoinList(joins.stream().map(j -> (RisingWaveExpression) j).collect(Collectors.toList()));
    }

    @Override
    public List<RisingWaveJoin> getJoinClauses() {
        return getJoinList().stream().map(j -> (RisingWaveJoin) j).collect(Collectors.toList());
    }

    @Override
    public RisingWaveType getType() {
        return getFetchColumns().get(0).getType();
    }

    @Override
    public String asString() {
        return RisingWaveToStringVisitor.asString(this);
    }
}
