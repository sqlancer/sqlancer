package sqlancer.questdb.ast;

import sqlancer.common.ast.newast.Node;

public class QuestDBConstant implements Node<QuestDBExpression> {
    private QuestDBConstant() {
    }

    public static class QuestDBNullConstant extends QuestDBConstant {
        @Override
        public String toString() {
            return "NULL";
        }
    }

    public static class QuestDBIntConstant extends QuestDBConstant {
        private final long value;

        public QuestDBIntConstant(long value) {
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

    public static class QuestDBBooleanConstant extends QuestDBConstant {
        private final boolean value;

        public QuestDBBooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public boolean getValue() {
            return value;
        }
    }

    public static Node<QuestDBExpression> createIntConstant(long val) {
        return new QuestDBIntConstant(val);
    }

    public static class QuestDBDoubleConstant extends QuestDBConstant {

        private final double value;

        public QuestDBDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "cast('Infinity' as double)";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "cast('-Infinity' as double)";
            }
            return String.valueOf(value);
        }

    }

    public static Node<QuestDBExpression> createBooleanConstant(boolean val) {
        return new QuestDBBooleanConstant(val);
    }

    public static Node<QuestDBExpression> createNullConstant() {
        return new QuestDBNullConstant();
    }

    public static Node<QuestDBExpression> createFloatConstant(double val) {
        return new QuestDBDoubleConstant(val);
    }
}
