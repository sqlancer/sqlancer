package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.yugabyte.ycql.YCQLSchema;

public class YCQLColumnReference extends ColumnReferenceNode<YCQLExpression, YCQLSchema.YCQLColumn>
        implements YCQLExpression {
    public YCQLColumnReference(YCQLSchema.YCQLColumn column) {
        super(column);
    }

}
