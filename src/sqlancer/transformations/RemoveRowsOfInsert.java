package sqlancer.transformations;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.values.ValuesStatement;

/**
 * This Transformer remove rows of insert. For example: INSERT INTO t1(c2, c0) VALUES (1508438260, 2929), (1508438260,
 * TIMESTAMP '1969-12-26 01:57:21'), (0.5347171705591047, 398662142); -> INSERT INTO t1 (c2, c0) VALUES
 * (0.5347171705591047, 398662142); might be reduced to INSERT INTO rt0 VALUES ('A');
 */
public class RemoveRowsOfInsert extends JSQLParserBasedTransformation {
    public RemoveRowsOfInsert() {
        super("remove rows of an insert statement");
    }

    @Override
    public void apply() {
        super.apply();
        if (statement instanceof Insert) {
            Insert insert = (Insert) statement;
            SelectBody selectBody = insert.getSelect().getSelectBody();
            if (selectBody instanceof SetOperationList) {
                SetOperationList insertingList = (SetOperationList) selectBody;
                for (SelectBody selBody : insertingList.getSelects()) {
                    if (selBody instanceof ValuesStatement) {
                        ValuesStatement valuesStatement = (ValuesStatement) selBody;
                        if (valuesStatement.getExpressions() instanceof ExpressionList) {
                            ExpressionList itemsList = (ExpressionList) valuesStatement.getExpressions();
                            tryRemoveElms(itemsList, itemsList.getExpressions(), ExpressionList::setExpressions);
                        }
                    }
                }
            }
        }
    }
}
