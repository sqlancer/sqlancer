package sqlancer.yugabyte.ysql.ast;

import java.util.List;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLCaseExpression implements YSQLExpression {

    private final YSQLExpression switchCondition; // Optional for searched CASE
    private final List<YSQLExpression> conditions;
    private final List<YSQLExpression> results;
    private final YSQLExpression elseResult;
    private final boolean isSimpleCase;

    public YSQLCaseExpression(YSQLExpression switchCondition, List<YSQLExpression> conditions,
                            List<YSQLExpression> results, YSQLExpression elseResult) {
        if (conditions.size() != results.size()) {
            throw new IllegalArgumentException("conditions and results must have the same size");
        }
        this.switchCondition = switchCondition;
        this.conditions = conditions;
        this.results = results;
        this.elseResult = elseResult;
        this.isSimpleCase = switchCondition != null;
    }

    public static YSQLCaseExpression createSearchedCase(List<YSQLExpression> conditions,
                                                       List<YSQLExpression> results,
                                                       YSQLExpression elseResult) {
        return new YSQLCaseExpression(null, conditions, results, elseResult);
    }

    public static YSQLCaseExpression createSimpleCase(YSQLExpression switchCondition,
                                                      List<YSQLExpression> conditions,
                                                      List<YSQLExpression> results,
                                                      YSQLExpression elseResult) {
        return new YSQLCaseExpression(switchCondition, conditions, results, elseResult);
    }

    @Override
    public YSQLDataType getExpressionType() {
        // The type is determined by the result expressions
        if (!results.isEmpty()) {
            return results.get(0).getExpressionType();
        } else if (elseResult != null) {
            return elseResult.getExpressionType();
        }
        return null;
    }

    public YSQLExpression getSwitchCondition() {
        return switchCondition;
    }

    public List<YSQLExpression> getConditions() {
        return conditions;
    }

    public List<YSQLExpression> getResults() {
        return results;
    }

    public YSQLExpression getElseResult() {
        return elseResult;
    }

    public boolean isSimpleCase() {
        return isSimpleCase;
    }
}