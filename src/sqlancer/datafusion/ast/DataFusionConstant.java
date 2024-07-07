package sqlancer.datafusion.ast;

import sqlancer.common.ast.newast.Node;

public class DataFusionConstant implements Node<DataFusionExpression> {

    private DataFusionConstant() {
    }

    public static Node<DataFusionExpression> createIntConstant(long val) {
        return new DataFusionIntConstant(val);
    }

    public static Node<DataFusionExpression> createNullConstant() {
        return new DataFusionNullConstant();
    }

    public static class DataFusionNullConstant extends DataFusionConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class DataFusionIntConstant extends DataFusionConstant {

        private final long value;

        public DataFusionIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

    }

    public static class DataFusionDoubleConstant extends DataFusionConstant {

        private final String valueStr;

        public DataFusionDoubleConstant(double value) {
            if (value == Double.POSITIVE_INFINITY) {
                valueStr = "'+Inf'::Double";
            } else if (value == Double.NEGATIVE_INFINITY) {
                valueStr = "'-Inf'::Double";
            } else if (Double.isNaN(value)) {
                valueStr = "'NaN'::Double";
            } else if (Double.compare(value, -0.0) == 0) {
                valueStr = "-0.0";
            } else {
                valueStr = String.valueOf(value);
            }
        }

        // Make it more convenient to construct special value like -0, NaN, etc.
        public DataFusionDoubleConstant(String valueStr) {
            this.valueStr = valueStr;
        }

        @Override
        public String toString() {
            return valueStr;
        }

    }

    public static class DataFusionBooleanConstant extends DataFusionConstant {

        private final boolean value;

        public DataFusionBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value) {
                return "true";
            } else {
                return "false";
            }
        }

    }

}
