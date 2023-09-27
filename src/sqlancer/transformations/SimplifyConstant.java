package sqlancer.transformations;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

/**
 * Shorten the constant of a statement e.g. "a_very_long_str" -> "_", 12341234->1.
 *
 * Note: The API of JSQLParser may have some problems with double values: `setValue` can't change the literal value of a
 * DoubleValue object. Therefore, double values are handled at RoundDoubleConstant class.
 */
public class SimplifyConstant extends JSQLParserBasedTransformation {
    static class ConstantCollector extends ExpressionDeParser {
        private final List<Expression> candidates = new ArrayList<>();

        @Override
        public void visit(DoubleValue doubleValue) {
            candidates.add(doubleValue);
            super.visit(doubleValue);
        }

        @Override
        public void visit(LongValue longValue) {
            candidates.add(longValue);
            super.visit(longValue);
        }

        @Override
        public void visit(StringValue stringValue) {
            candidates.add(stringValue);
            super.visit(stringValue);
        }

        public List<Expression> getCandidates() {
            return candidates;
        }
    }

    public SimplifyConstant() {
        super("simplify constant expressions");
    }

    @Override
    public void apply() {
        super.apply();
        ConstantCollector collector = new ConstantCollector();
        StringBuilder buffer = new StringBuilder();
        SelectDeParser collectorDeParser = new SelectDeParser(collector, buffer);
        collector.setSelectVisitor(collectorDeParser);
        collector.setBuffer(buffer);

        List<Expression> candidates = collector.getCandidates();

        StatementVisitorAdapter statementVisitor = new StatementVisitorAdapter() {
            @Override
            public void visit(Insert insert) {
                insert.getSelect().getSelectBody().accept(collectorDeParser);
                super.visit(insert);
            }

            @Override
            public void visit(Select select) {
                select.getSelectBody().accept(collectorDeParser);
                super.visit(select);
            }
        };

        statement.accept(statementVisitor);

        for (Expression e : candidates) {
            if (e instanceof LongValue) {
                simplify((LongValue) e);
            } else if (e instanceof StringValue) {
                simplify((StringValue) e);
            }
        }
    }

    private void simplify(LongValue longValue) {
        long variant = 0;
        if (!longValue.getStringValue().equals(String.valueOf(variant))) {
            tryReplace(longValue, longValue.getStringValue(), String.valueOf(variant), LongValue::setStringValue);
        }
    }

    private void simplify(StringValue stringValue) {
        String variant = "_";
        if (!stringValue.getValue().equals(variant)) {
            tryReplace(stringValue, stringValue.getValue(), variant, StringValue::setValue);
        }
    }

}
