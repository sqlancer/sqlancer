package sqlancer.mysql.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.common.ast.SelectBase;

public class MySQLSelect extends SelectBase<MySQLExpression> implements MySQLExpression {

    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();

    public enum SelectType {
        DISTINCT, ALL, DISTINCTROW;
    }

    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getSelectType() {
        return fromOptions;
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

}
