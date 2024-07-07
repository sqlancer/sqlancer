package sqlancer.datafusion.test;

import static sqlancer.datafusion.DataFusionUtil.DataFusionLogger.DataFusionLogType.ERROR;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.DataFusionUtil;
import sqlancer.datafusion.ast.DataFusionSelect;

public class DataFusionQueryPartitioningWhereTester extends DataFusionQueryPartitioningBase {
    public DataFusionQueryPartitioningWhereTester(DataFusionGlobalState state) {
        super(state);
        DataFusionErrors.registerExpectedExecutionErrors(errors);
    }

    /*
     * Query Partitioning - Where q: SELECT [expr1] FROM [expr2] qp1: SELECT [expr1] FROM [expr2] WHERE [expr3] qp2:
     * SELECT [expr1] FROM [expr2] WHERE NOT [expr3] qp3: SELECT [expr1] FROM [expr2] WHERE [expr3] IS NULL
     *
     * Oracle check: q's result equals to union(qp1, qp2, qp3)
     */
    @Override
    public void check() throws SQLException {
        // generate a random 'SELECT [expr1] FROM [expr2] WHERE [expr3]
        super.check();
        DataFusionSelect randomSelect = select;
        randomSelect.setWhereClause(null);

        // Construct q
        String qString = DataFusionToStringVisitor.asString(randomSelect);
        // Construct qp1, qp2, qp3
        randomSelect.setWhereClause(predicate);
        String qp1String = DataFusionToStringVisitor.asString(randomSelect);
        randomSelect.setWhereClause(negatedPredicate);
        String qp2String = DataFusionToStringVisitor.asString(randomSelect);
        randomSelect.setWhereClause(isNullPredicate);
        String qp3String = DataFusionToStringVisitor.asString(randomSelect);

        try {
            /*
             * Run all queires
             */
            List<String> qResultSet = ComparatorHelper.getResultSetFirstColumnAsString(qString, errors, state);
            List<String> combinedString = new ArrayList<>();
            List<String> qpResultSet = ComparatorHelper.getCombinedResultSet(qp1String, qp2String, qp3String,
                    combinedString, true, state, errors);
            /*
             * Query Partitioning-Where check
             */
            ComparatorHelper.assumeResultSetsAreEqual(qResultSet, qpResultSet, qString, combinedString, state,
                    ComparatorHelper::canonicalizeResultValue);
        } catch (AssertionError e) {
            // Append more error message
            String replay = DataFusionUtil.getReplay(state.getDatabaseName());
            String newMessage = e.getMessage() + "\n" + e.getCause() + "\n" + replay + "\n";
            state.dfLogger.appendToLog(ERROR, newMessage);

            throw new AssertionError(newMessage);
        }
    }
}
