package sqlancer.oceanbase.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.common.ast.SelectBase;

public class OceanBaseSelect extends SelectBase<OceanBaseExpression> implements OceanBaseExpression {

    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();
    private List<OceanBaseExpression> groupBys = new ArrayList<>();
    private OceanBaseStringExpression hint;

    public enum SelectType {
        DISTINCT, ALL;
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

    public void setGroupByClause(List<OceanBaseExpression> groupBys) {
        this.groupBys = groupBys;
    }

    public List<OceanBaseExpression> getGroupByClause() {
        return this.groupBys;
    }

    public void setModifiers(List<String> modifiers) {
        this.modifiers = modifiers;
    }

    public List<String> getModifiers() {
        return modifiers;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        return null;
    }

    public void setHint(OceanBaseStringExpression hint) {
        this.hint = hint;
    }

    public OceanBaseStringExpression getHint() {
        return hint;
    }

}
