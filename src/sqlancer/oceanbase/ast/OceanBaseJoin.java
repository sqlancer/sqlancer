package sqlancer.oceanbase.ast;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;

public class OceanBaseJoin extends JoinBase<OceanBaseExpression> implements OceanBaseExpression, Join<OceanBaseExpression, OceanBaseTable, OceanBaseColumn> {

    public OceanBaseJoin(OceanBaseExpression tableReference, OceanBaseExpression onClause, JoinType type) {
        super(tableReference, onClause, type);
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOnClause(OceanBaseExpression onClause) {
    }
}
