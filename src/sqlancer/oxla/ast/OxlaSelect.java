package sqlancer.oxla.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.oxla.OxlaToStringVisitor;
import sqlancer.oxla.schema.OxlaColumn;
import sqlancer.oxla.schema.OxlaTable;

import java.util.List;
import java.util.stream.Collectors;

public class OxlaSelect extends SelectBase<OxlaExpression>
        implements OxlaExpression, Select<OxlaJoin, OxlaExpression, OxlaTable, OxlaColumn> {
    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public SelectType type;
    public OxlaExpression distinctOnClause = null;

    @Override
    public void setJoinClauses(List<OxlaJoin> joinStatements) {
        setJoinList(joinStatements
                .stream()
                .map(e -> (OxlaExpression) e)
                .collect(Collectors.toList()));
    }

    @Override
    public List<OxlaJoin> getJoinClauses() {
        return getJoinList()
                .stream()
                .map(e -> (OxlaJoin) e)
                .collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return OxlaToStringVisitor.asString(this);
    }

    @Override
    public String toString() {
        return asString();
    }
}
