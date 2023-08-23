package sqlancer.transformations;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.values.ValuesStatement;

/**
 * This Transformer remove rows of insert. Given a sql statement:
 *
 * INSERT INTO t1(c2, c0) VALUES (1508438260, 2929), (1508438260, TIMESTAMP '1969-12-26 01:57:21'), (0.5347171705591047,
 * 398662142); -> INSERT INTO t1 (c2, c0) VALUES (0.5347171705591047, 398662142);
 */
public class RemoveRowsOfInsert extends JSQLParserBasedTransformation {
    public RemoveRowsOfInsert() {
        super("remove rows of an insert statement");
    }

    @Override
    public void apply() {
        super.apply();
        if (!(statement instanceof Insert)) {
            return;
        }
        SelectBody selectBody = ((Insert) statement).getSelect().getSelectBody();
        if (!(selectBody instanceof SetOperationList)) {
            return;
        }
        SetOperationList insertingList = (SetOperationList) selectBody;
        for (SelectBody selBody : insertingList.getSelects()) {
            if (!(selBody instanceof ValuesStatement)) {
                continue;
            }
            ValuesStatement valuesStatement = (ValuesStatement) selBody;
            ItemsList itemsList = valuesStatement.getExpressions();
            if (!(itemsList instanceof ExpressionList)) {
                continue;
            }
            tryRemoveElms((ExpressionList) itemsList, ((ExpressionList) itemsList).getExpressions(),
                    ExpressionList::setExpressions);
        }
    }
}
