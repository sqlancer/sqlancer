package sqlancer.transformations;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 * Transformations based on JSQLParser should be derived from this class.
 */

public class JSQLParserBasedTransformation extends Transformation {

    protected Statement statement;

    public JSQLParserBasedTransformation(String desc) {
        super(desc);
    }

    @Override
    protected void onStatementChanged() {
        if (statementChangedHandler != null) {
            statementChangedHandler.accept(this.statement.toString());
        }
    }

    @Override
    public boolean init(String sql) {
        this.current = sql;
        try {
            statement = CCJSqlParserUtil.parse(current);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

}
