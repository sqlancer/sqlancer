package sqlancer.arangodb.ast;

import com.arangodb.entity.BaseDocument;

import sqlancer.common.ast.newast.Node;

public abstract class ArangoDBConstant implements Node<ArangoDBExpression> {
    private ArangoDBConstant() {

    }

    public abstract void setValueInDocument(BaseDocument document, String key);

    public abstract Object getValue();

    public static class ArangoDBIntegerConstant extends ArangoDBConstant {

        private final int value;

        public ArangoDBIntegerConstant(int value) {
            this.value = value;
        }

        @Override
        public void setValueInDocument(BaseDocument document, String key) {
            document.addAttribute(key, value);
        }

        @Override
        public Object getValue() {
            return value;
        }
    }

    public static Node<ArangoDBExpression> createIntegerConstant(int value) {
        return new ArangoDBIntegerConstant(value);
    }

    public static class ArangoDBStringConstant extends ArangoDBConstant {
        private final String value;

        public ArangoDBStringConstant(String value) {
            this.value = value;
        }

        @Override
        public void setValueInDocument(BaseDocument document, String key) {
            document.addAttribute(key, value);
        }

        @Override
        public Object getValue() {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }
    }

    public static Node<ArangoDBExpression> createStringConstant(String value) {
        return new ArangoDBStringConstant(value);
    }

    public static class ArangoDBBooleanConstant extends ArangoDBConstant {
        private final boolean value;

        public ArangoDBBooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public void setValueInDocument(BaseDocument document, String key) {
            document.addAttribute(key, value);
        }

        @Override
        public Object getValue() {
            return value;
        }
    }

    public static Node<ArangoDBExpression> createBooleanConstant(boolean value) {
        return new ArangoDBBooleanConstant(value);
    }

    public static class ArangoDBDoubleConstant extends ArangoDBConstant {
        private final double value;

        public ArangoDBDoubleConstant(double value) {
            if (Double.isInfinite(value) || Double.isNaN(value)) {
                this.value = 0.0;
            } else {
                this.value = value;
            }
        }

        @Override
        public void setValueInDocument(BaseDocument document, String key) {
            document.addAttribute(key, value);
        }

        @Override
        public Object getValue() {
            return value;
        }
    }

    public static Node<ArangoDBExpression> createDoubleConstant(double value) {
        return new ArangoDBDoubleConstant(value);
    }
}
