package sqlancer.oxla.ast;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaTable;

import java.util.List;

public class OxlaSelect extends SelectBase<OxlaExpression>
        implements OxlaExpression, Select<OxlaJoin, OxlaExpression, OxlaTable, OxlaColumn> {
    public enum SelectType {DISTINCT, ALL}

    public SelectType type = SelectType.ALL;

    @Override
    public void setJoinClauses(List<OxlaJoin> joinStatements) {

    }

    @Override
    public List<OxlaJoin> getJoinClauses() {
        return List.of();
    }

    @Override
    public String asString() {
        return "";
    }
}
