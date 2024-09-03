package sqlancer.doris.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisTable;
import sqlancer.doris.visitor.DorisToStringVisitor;

public class DorisSelect extends SelectBase<DorisExpression>
        implements DorisExpression, Select<DorisJoin, DorisExpression, DorisTable, DorisColumn> {

    public enum DorisSelectDistinctType {

        ALL, DISTINCT, DISTINCTROW, NULL;

        public static DorisSelectDistinctType getRandomWithoutNull() {
            DorisSelectDistinctType sft;
            do {
                sft = Randomly.fromOptions(values());
            } while (sft == DorisSelectDistinctType.NULL);
            return sft;
        }
    }

    private DorisSelectDistinctType selectDistinctType = DorisSelectDistinctType.ALL;

    public void setDistinct(boolean isDistinct) {
        if (isDistinct) {
            this.selectDistinctType = DorisSelectDistinctType.DISTINCT;
        } else {
            this.selectDistinctType = DorisSelectDistinctType.ALL;
        }
    }

    public void setDistinct(DorisSelectDistinctType type) {
        this.selectDistinctType = type;
    }

    public boolean isDistinct() {
        return this.selectDistinctType == DorisSelectDistinctType.DISTINCT
                || this.selectDistinctType == DorisSelectDistinctType.DISTINCTROW;
    }

    @Override
    public void setJoinClauses(List<DorisJoin> joinStatements) {
        List<DorisExpression> expressions = joinStatements.stream().map(e -> (DorisExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<DorisJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (DorisJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return DorisToStringVisitor.asString(this);
    }
}
