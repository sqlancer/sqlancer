package sqlancer.tidb.ast;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBSelect extends SelectBase<TiDBExpression>
        implements TiDBExpression, Select<TiDBJoin, TiDBExpression, TiDBTable, TiDBColumn> {

    private TiDBExpression hint;
    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();

    public enum SelectType {
        DISTINCT, ALL, DISTINCTROW
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
    public TiDBConstant getExpectedValue() {
        return null;
    }

    public void setHint(TiDBExpression hint) {
        this.hint = hint;
    }

    public TiDBExpression getHint() {
        return hint;
    }

    @Override
    public void setJoinClauses(List<TiDBJoin> joinStatements) {
        List<TiDBExpression> expressions = joinStatements.stream().map(e -> (TiDBExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<TiDBJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (TiDBJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return TiDBVisitor.asString(this);
    }
}
