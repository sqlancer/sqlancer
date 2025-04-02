package sqlancer.hive.ast;

import java.util.List;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setJoinClauses'");
    }

    @Override
    public List<HiveJoin> getJoinClauses() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getJoinClauses'");
    }

    @Override
    public String asString() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'asString'");
    }

}
