package sqlancer.postgres.ast;

import java.util.List;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresWindowFunction implements PostgresExpression {

    private final String functionName;
    private final List<PostgresExpression> arguments;
    private final WindowSpecification windowSpec;
    private final PostgresDataType returnType;

    public PostgresWindowFunction(String functionName, List<PostgresExpression> arguments,
            WindowSpecification windowSpec, PostgresDataType returnType) {
        this.functionName = functionName;
        this.arguments = arguments;
        this.windowSpec = windowSpec;
        this.returnType = returnType;
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<PostgresExpression> getArguments() {
        return arguments;
    }

    public WindowSpecification getWindowSpec() {
        return windowSpec;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return returnType;
    }

    public static class WindowSpecification {
        private final List<PostgresExpression> partitionBy;
        private final List<PostgresOrderByTerm> orderBy;
        private final WindowFrame frame;

        public WindowSpecification(List<PostgresExpression> partitionBy, List<PostgresOrderByTerm> orderBy,
                WindowFrame frame) {
            this.partitionBy = partitionBy;
            this.orderBy = orderBy;
            this.frame = frame;
        }

        public List<PostgresExpression> getPartitionBy() {
            return partitionBy;
        }

        public List<PostgresOrderByTerm> getOrderBy() {
            return orderBy;
        }

        public WindowFrame getFrame() {
            return frame;
        }
    }

    public static class WindowFrame {
        public enum FrameType {
            ROWS("ROWS"), RANGE("RANGE");

            private final String sql;

            FrameType(String sql) {
                this.sql = sql;
            }

            public String getSQL() {
                return sql;
            }
        }

        private final FrameType type;
        private final PostgresExpression startExpr;
        private final PostgresExpression endExpr;

        public WindowFrame(FrameType type, PostgresExpression startExpr, PostgresExpression endExpr) {
            this.type = type;
            this.startExpr = startExpr;
            this.endExpr = endExpr;
        }

        public FrameType getType() {
            return type;
        }

        public PostgresExpression getStartExpr() {
            return startExpr;
        }

        public PostgresExpression getEndExpr() {
            return endExpr;
        }
    }
}
