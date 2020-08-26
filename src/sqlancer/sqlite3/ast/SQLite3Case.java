package sqlancer.sqlite3.ast;

import java.util.Optional;

import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public abstract class SQLite3Case extends SQLite3Expression {

    protected final CasePair[] pairs;
    protected final SQLite3Expression elseExpr;

    public SQLite3Case(CasePair[] pairs, SQLite3Expression elseExpr) {
        this.pairs = pairs.clone();
        this.elseExpr = elseExpr;
    }

    public static class CasePair {

        private final SQLite3Expression cond;
        private final SQLite3Expression then;

        public CasePair(SQLite3Expression cond, SQLite3Expression then) {
            this.cond = cond;
            this.then = then;
        }

        public SQLite3Expression getCond() {
            return cond;
        }

        public SQLite3Expression getThen() {
            return then;
        }
    }

    public CasePair[] getPairs() {
        return pairs.clone();
    }

    public SQLite3Expression getElseExpr() {
        return elseExpr;
    }

    protected SQLite3CollateSequence getExplicitCasePairAndElseCollate() {
        for (CasePair c : pairs) {
            if (c.getCond().getExplicitCollateSequence() != null) {
                return c.getCond().getExplicitCollateSequence();
            } else if (c.getThen().getExplicitCollateSequence() != null) {
                return c.getThen().getExplicitCollateSequence();
            }
        }
        if (elseExpr == null) {
            return null;
        } else {
            return elseExpr.getExplicitCollateSequence();
        }
    }

    public static class SQLite3CaseWithoutBaseExpression extends SQLite3Case {

        public SQLite3CaseWithoutBaseExpression(CasePair[] pairs, SQLite3Expression elseExpr) {
            super(pairs, elseExpr);
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            return getExplicitCasePairAndElseCollate();
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            for (CasePair c : pairs) {
                SQLite3Constant expectedValue = c.getCond().getExpectedValue();
                if (expectedValue == null) {
                    return null;
                }
                Optional<Boolean> isTrue = SQLite3Cast.isTrue(expectedValue);
                if (isTrue.isPresent() && isTrue.get()) {
                    return c.getThen().getExpectedValue();
                }
            }
            if (elseExpr == null) {
                return SQLite3Constant.createNullConstant();
            } else {
                return elseExpr.getExpectedValue();
            }
        }

    }

    public static class SQLite3CaseWithBaseExpression extends SQLite3Case {

        private final SQLite3Expression baseExpr;

        public SQLite3CaseWithBaseExpression(SQLite3Expression baseExpr, CasePair[] pairs, SQLite3Expression elseExpr) {
            super(pairs, elseExpr);
            this.baseExpr = baseExpr;
        }

        @Override
        public SQLite3CollateSequence getExplicitCollateSequence() {
            if (baseExpr.getExplicitCollateSequence() != null) {
                return baseExpr.getExplicitCollateSequence();
            } else {
                return getExplicitCasePairAndElseCollate();
            }
        }

        public SQLite3Expression getBaseExpr() {
            return baseExpr;
        }

        @Override
        public SQLite3Constant getExpectedValue() {
            SQLite3Constant baseExprValue = baseExpr.getExpectedValue();
            if (baseExprValue == null) {
                return null;
            }
            for (CasePair c : pairs) {
                SQLite3Constant whenComparisonValue = c.getCond().getExpectedValue();
                if (whenComparisonValue == null) {
                    return null;
                }
                SQLite3CollateSequence seq;
                if (baseExpr.getExplicitCollateSequence() != null) {
                    seq = baseExpr.getExplicitCollateSequence();
                } else if (c.getCond().getExplicitCollateSequence() != null) {
                    seq = c.getCond().getExplicitCollateSequence();
                } else if (c.getCond().getImplicitCollateSequence() != null) {
                    seq = c.getCond().getImplicitCollateSequence();
                } else if (c.getCond().getImplicitCollateSequence() != null) {
                    seq = c.getCond().getImplicitCollateSequence();
                } else {
                    seq = SQLite3CollateSequence.BINARY;
                }
                ConstantTuple newVals = applyAffinities(baseExpr.getAffinity(), c.getCond().getAffinity(),
                        baseExpr.getExpectedValue(), c.getCond().getExpectedValue());
                SQLite3Constant equals = newVals.left.applyEquals(newVals.right, seq);
                if (!equals.isNull() && equals.asInt() == 1) {
                    return c.getThen().getExpectedValue();
                }
            }
            if (elseExpr == null) {
                return SQLite3Constant.createNullConstant();
            } else {
                return elseExpr.getExpectedValue();
            }
        }

    }
}
