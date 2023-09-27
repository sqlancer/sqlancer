package sqlancer.transformations;

import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

/**
 * remove columns of a select: e.g. select a, b, c from t -> select a from t.
 */
public class RemoveColumnsOfSelect extends JSQLParserBasedTransformation {

    private final SelectDeParser remover = new SelectDeParser() {
        @Override
        public void visit(PlainSelect plainSelect) {
            tryRemoveElms(plainSelect, plainSelect.getSelectItems(), PlainSelect::setSelectItems);
            super.visit(plainSelect);
        }
    };

    public RemoveColumnsOfSelect() {
        super("remove columns of a select");
    }

    @Override
    public boolean init(String original) {

        boolean baseSucc = super.init(original);
        if (!baseSucc) {
            return false;
        }
        this.remover.setExpressionVisitor(new ExpressionDeParser(remover, new StringBuilder()));
        return true;
    }

    @Override
    public void apply() {
        super.apply();
        if (statement instanceof Select) {
            Select select = (Select) statement;
            select.getSelectBody().accept(remover);

            List<WithItem> withItemsList = select.getWithItemsList();
            if (withItemsList == null) {
                return;
            }
            for (WithItem withItem : withItemsList) {
                SubSelect subSelect = withItem.getSubSelect();
                if (subSelect == null) {
                    return;
                }

                if (subSelect.getSelectBody() != null) {
                    subSelect.getSelectBody().accept(remover);
                }
            }

        }
    }
}
