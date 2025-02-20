package sqlancer.datafusion.ast;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.DataFusionToStringVisitor;

public class DataFusionSelect extends SelectBase<DataFusionExpression> implements DataFusionExpression,
        Select<DataFusionJoin, DataFusionExpression, DataFusionTable, DataFusionColumn> {
    public Optional<String> fetchColumnsString = Optional.empty(); // When available, override `fetchColumns` in base

    /*
     * If set fetch columns with string It will override `fetchColumns` in base class when
     * `DataFusionToStringVisitor.asString()` is called
     *
     * This method can be helpful to mutate select in oracle checks: SELECT [expr] ... -> SELECT SUM[expr]
     */
    public void setFetchColumnsString(String selectExpr) {
        this.fetchColumnsString = Optional.of(selectExpr);
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public void setJoinClauses(List<JoinBase<DataFusionExpression>> joinStatements) {
        List<DataFusionExpression> expressions = joinStatements.stream().map(e -> (DataFusionExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<JoinBase<DataFusionExpression>> getJoinClauses() {
        return getJoinList().stream().map(e -> (JoinBase<DataFusionExpression>) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return DataFusionToStringVisitor.asString(this);
    }
}
