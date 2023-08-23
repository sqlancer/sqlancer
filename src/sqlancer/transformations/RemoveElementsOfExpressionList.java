package sqlancer.transformations;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.InsertDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

/**
 * remove elements of an expression list.
 *
 * NOTE: this only works for select statements and targets at ExpressionList type in JSQLParser, such as groupBy list
 */
public class RemoveElementsOfExpressionList extends JSQLParserBasedTransformation {
    private final ExpressionDeParser expressionHandler = new ExpressionDeParser();
    private final SelectDeParser simplifier = new SelectDeParser() {
        @Override
        public void visit(PlainSelect plainSelect) {
            handleSelect(plainSelect);
            super.visit(plainSelect);
        }

        @Override
        public void visit(ExpressionList expressionList) {
            List<Expression> expressions = expressionList.getExpressions();
            tryRemoveElms(expressionList, expressions, ExpressionList::setExpressions);
            super.visit(expressionList);
        }
    };
    private final InsertDeParser insertDeParser = new InsertDeParser() {
        @Override
        public void visit(ExpressionList expressionList) {
            List<Expression> expressions = expressionList.getExpressions();
            tryRemoveElms(expressionList, expressions, ExpressionList::setExpressions);
            super.visit(expressionList);
        }
    };

    public RemoveElementsOfExpressionList() {
        super("remove elements of expression lists");
    }

    @Override
    public boolean init(String sql) {
        boolean baseSuc = super.init(sql);
        if (!baseSuc) {
            return false;
        }
        this.simplifier.setExpressionVisitor(expressionHandler);
        this.expressionHandler.setSelectVisitor(simplifier);

        this.insertDeParser.setExpressionVisitor(expressionHandler);
        this.insertDeParser.setSelectVisitor(simplifier);
        return true;
    }

    @Override
    public void apply() {
        super.apply();
        if (statement instanceof Select) {
            Select select = (Select) statement;
            select.getSelectBody().accept(simplifier);
        }
    }

    private void handleSelect(PlainSelect plainSelect) {

        GroupByElement groupByElement = plainSelect.getGroupBy();

        if (groupByElement != null && groupByElement.getGroupByExpressionList() != null) {
            ExpressionList expressionList = groupByElement.getGroupByExpressionList();
            List<Expression> list = expressionList.getExpressions();
            tryRemoveElms(expressionList, list, ExpressionList::setExpressions);
        }

        List<Join> expressionList = plainSelect.getJoins();
        if (expressionList != null) {
            tryRemoveElms(plainSelect, expressionList, PlainSelect::setJoins);
        }
    }
}
