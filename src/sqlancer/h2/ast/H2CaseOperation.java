package sqlancer.h2.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewCaseOperatorNode;

public class H2CaseOperation extends NewCaseOperatorNode<H2Expression> implements H2Expression {
    public H2CaseOperation(H2Expression switchCondition, List<H2Expression> conditions, List<H2Expression> expressions,
            H2Expression elseExpr) {
        super(switchCondition, conditions, expressions, elseExpr);
    }
}
