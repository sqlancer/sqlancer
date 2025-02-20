package sqlancer.mysql.ast;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLVisitor;

public class MySQLSelect extends SelectBase<MySQLExpression>
        implements MySQLExpression, Select<MySQLJoin, MySQLExpression, MySQLTable, MySQLColumn> {

    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();
    private MySQLText hint;

    public enum SelectType {
        DISTINCT, ALL, DISTINCTROW;
    }

    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    public void setModifiers(List<String> modifiers) {
        this.modifiers = modifiers;
    }

    public List<String> getModifiers() {
        return modifiers;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;
    }

    public void setHint(MySQLText hint) {
        this.hint = hint;
    }

    public MySQLText getHint() {
        return hint;
    }

    @Override
    public void setJoinClauses(List<JoinBase<MySQLExpression>> joinStatements) {
        List<MySQLExpression> expressions = joinStatements.stream().map(e -> (MySQLExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<JoinBase<MySQLExpression>> getJoinClauses() {
        return getJoinList().stream().map(e -> (JoinBase<MySQLExpression>) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return MySQLVisitor.asString(this);
    }
}
