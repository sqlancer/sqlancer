package sqlancer.hsqldb.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBColumn;
import sqlancer.hsqldb.HSQLDBSchema.HSQLDBTable;
import sqlancer.hsqldb.HSQLDBToStringVisitor;

public class HSQLDBSelect extends SelectBase<HSQLDBExpression>
        implements HSQLDBExpression, Select<HSQLDBJoin, HSQLDBExpression, HSQLDBTable, HSQLDBColumn> {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    @Override
    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public void setJoinClauses(List<JoinBase<HSQLDBExpression>> joinStatements) {
        List<HSQLDBExpression> expressions = joinStatements.stream().map(e -> (HSQLDBExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<JoinBase<HSQLDBExpression>> getJoinClauses() {
        return getJoinList().stream().map(e -> (JoinBase<HSQLDBExpression>) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return HSQLDBToStringVisitor.asString(this);
    }
}
