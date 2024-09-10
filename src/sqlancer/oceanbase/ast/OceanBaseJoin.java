package sqlancer.oceanbase.ast;

import sqlancer.common.ast.newast.Join;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;

public class OceanBaseJoin implements OceanBaseExpression, Join<OceanBaseExpression, OceanBaseTable, OceanBaseColumn> {

    @Override
    public OceanBaseConstant getExpectedValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOnClause(OceanBaseExpression onClause) {
    }
}
