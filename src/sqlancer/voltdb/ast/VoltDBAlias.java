package sqlancer.voltdb.ast;

import sqlancer.common.ast.newast.NewAliasNode;

// "expression AS alias" in a SELECT list.
public class VoltDBAlias extends NewAliasNode<VoltDBExpression> implements VoltDBExpression {

    public VoltDBAlias(VoltDBExpression expr, String alias) {
        super(expr, alias);
    }

}
