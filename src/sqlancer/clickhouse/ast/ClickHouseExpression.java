package sqlancer.clickhouse.ast;

import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.common.visitor.UnaryOperation;

public abstract class ClickHouseExpression {

    public ClickHouseConstant getExpectedValue() {
        return null;
    }

    public enum TypeAffinity {
        NOTHING, UINT8, UINT16, UINT32, UINT64, UINT128, INT8, INT16, INT32, INT64, INT128, FLOAT32, FLOAT64, DATE,
        DATETIME, DATETIME64, STRING, FIXEDSTRING, ENUM8, ENUM16, DECIMAL32, DECIMAL64, DECIMAL128, UUID, ARRAY, TUPLE,
        SET, INTERVAL;
        // NULLABLE, FUNCTION, AGGREGATEFUNCTION, LOWCARDINALITY;

        public boolean isNumeric() {
            return this == UINT8 || this == UINT16 || this == UINT32 || this == UINT64 || this == UINT128
                    || this == INT8 || this == INT16 || this == INT32 || this == INT64 || this == INT128
                    || this == FLOAT32 || this == FLOAT64;
        }
    }

    public static class ClickHouseExist extends ClickHouseExpression {

        private final ClickHouseExpression select;

        public ClickHouseExist(ClickHouseExpression select) {
            this.select = select;
        }

        public ClickHouseExpression getExpression() {
            return select;
        }
    }

    public static class ClickHouseJoin extends ClickHouseExpression {

        // TODO: support ANY, ALL, ASOF modifiers
        public enum JoinType {
            INNER, CROSS, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, NATURAL, LEFT_SEMI, RIGHT_SEMI, LEFT_ANTI, RIGHT_ANTI;
        }

        private final ClickHouseSchema.ClickHouseTable table;
        private ClickHouseExpression onClause;
        private final ClickHouseJoin.JoinType type;

        public ClickHouseJoin(ClickHouseSchema.ClickHouseTable table, ClickHouseExpression onClause,
                ClickHouseJoin.JoinType type) {
            this.table = table;
            this.onClause = onClause;
            this.type = type;
        }

        public ClickHouseJoin(ClickHouseSchema.ClickHouseTable table, ClickHouseJoin.JoinType type) {
            this.table = table;
            if (type != ClickHouseJoin.JoinType.NATURAL) {
                throw new AssertionError();
            }
            this.onClause = null;
            this.type = type;
        }

        public ClickHouseSchema.ClickHouseTable getTable() {
            return table;
        }

        public ClickHouseExpression getOnClause() {
            return onClause;
        }

        public ClickHouseJoin.JoinType getType() {
            return type;
        }

        public void setOnClause(ClickHouseExpression onClause) {
            this.onClause = onClause;
        }

    }

    public static class ClickHouseSubquery extends ClickHouseExpression {

        private final String query;

        public ClickHouseSubquery(String query) {
            this.query = query;
        }

        public static ClickHouseExpression create(String query) {
            return new ClickHouseSubquery(query);
        }

        public String getQuery() {
            return query;
        }
    }

    public static class ClickHousePostfixText extends ClickHouseExpression
            implements UnaryOperation<ClickHouseExpression> {

        private final ClickHouseExpression expr;
        private final String text;
        private ClickHouseConstant expectedValue;

        public ClickHousePostfixText(ClickHouseExpression expr, String text, ClickHouseConstant expectedValue) {
            this.expr = expr;
            this.text = text;
            this.expectedValue = expectedValue;
        }

        public ClickHousePostfixText(String text, ClickHouseConstant expectedValue) {
            this(null, text, expectedValue);
        }

        public String getText() {
            return text;
        }

        @Override
        public ClickHouseConstant getExpectedValue() {
            return expectedValue;
        }

        @Override
        public ClickHouseExpression getExpression() {
            return expr;
        }

        @Override
        public String getOperatorRepresentation() {
            return getText();
        }

        @Override
        public OperatorKind getOperatorKind() {
            return OperatorKind.POSTFIX;
        }

        @Override
        public boolean omitBracketsWhenPrinting() {
            return true;
        }
    }
}
