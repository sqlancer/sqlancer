package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.newast.Join;
import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.MaterializeSchema.MaterializeTable;

public class MaterializeJoin extends JoinBase<MaterializeExpression>
        implements MaterializeExpression, Join<MaterializeExpression, MaterializeTable, MaterializeColumn> {

    public MaterializeJoin(MaterializeExpression tableReference, MaterializeExpression onClause,
            JoinType type) {
        super(tableReference, onClause, type);
    }

    public MaterializeExpression getTableReference() {
        return tableReference;
    }

    public MaterializeExpression getOnClause() {
        return onClause;
    }

    public JoinType getType() {
        return type;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        throw new AssertionError();
    }

    @Override
    public void setOnClause(MaterializeExpression onClause) {
        this.onClause = onClause;
    }
}
