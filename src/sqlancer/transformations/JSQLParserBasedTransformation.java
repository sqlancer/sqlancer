package sqlancer.transformations;

import java.util.function.Consumer;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 * Transformations based on JSQLParser should be derived from this class.
 */

public class JSQLParserBasedTransformation extends Transformation {

    protected Statement statement;
    protected Consumer<Statement> statementChangedHandler;

    public JSQLParserBasedTransformation(String desc) {
        super(desc);
    }

    public void setStatementChangedCallBack(Consumer<Statement> statementChangedHandler) {
        this.statementChangedHandler = statementChangedHandler;
    }

    @Override
    protected void onStatementChanged() {
        if (statementChangedHandler != null) {
            statementChangedHandler.accept(this.statement);
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
