package sqlancer.voltdb.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.VoltDBToStringVisitor;

public class VoltDBSelect extends SelectBase<VoltDBExpression>
        implements VoltDBExpression, Select<VoltDBJoin, VoltDBExpression, VoltDBTable, VoltDBColumn> {

    private boolean distinct;

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public void setJoinClauses(List<VoltDBJoin> joinStatements) {
        setJoinList(joinStatements.stream().map(j -> (VoltDBExpression) j).collect(Collectors.toList()));
    }

    @Override
    public List<VoltDBJoin> getJoinClauses() {
        return getJoinList().stream().map(j -> (VoltDBJoin) j).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return VoltDBToStringVisitor.asString(this);
    }

}
