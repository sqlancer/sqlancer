package sqlancer.sqlite3;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.sqlite3.SQLite3Options.SQLite3OracleFactory;
import sqlancer.sqlite3.oracle.SQLite3Fuzzer;
import sqlancer.sqlite3.oracle.SQLite3NoRECOracle;
import sqlancer.sqlite3.oracle.SQLite3PivotedQuerySynthesisOracle;
import sqlancer.sqlite3.oracle.tlp.SQLite3TLPAggregateOracle;
import sqlancer.sqlite3.oracle.tlp.SQLite3TLPDistinctOracle;
import sqlancer.sqlite3.oracle.tlp.SQLite3TLPGroupByOracle;
import sqlancer.sqlite3.oracle.tlp.SQLite3TLPHavingOracle;
import sqlancer.sqlite3.oracle.tlp.SQLite3TLPWhereOracle;

@Parameters(separators = "=", commandDescription = "SQLite3")
public class SQLite3Options implements DBMSSpecificOptions<SQLite3OracleFactory> {

    @Parameter(names = { "--test-fts" }, description = "Test the FTS extensions", arity = 1)
    public boolean testFts = true;

    @Parameter(names = { "--test-rtree" }, description = "Test the R*Tree extensions", arity = 1)
    public boolean testRtree = true;

    @Parameter(names = {
            "--test-dbstats" }, description = "Test the DBSTAT Virtual Table (see https://www.sqlite.org/dbstat.html)", arity = 1)
    public boolean testDBStats;

    @Parameter(names = { "--test-generated-columns" }, description = "Test generated columns", arity = 1)
    public boolean testGeneratedColumns = true;

    @Parameter(names = { "--test-foreign-keys" }, description = "Test foreign key constraints", arity = 1)
    public boolean testForeignKeys = true;

    @Parameter(names = { "--test-without-rowids" }, description = "Generate WITHOUT ROWID tables", arity = 1)
    public boolean testWithoutRowids = true;

    @Parameter(names = { "--test-temp-tables" }, description = "Generate TEMP/TEMPORARY tables", arity = 1)
    public boolean testTempTables = true;

    @Parameter(names = { "--test-check-constraints" }, description = "Allow CHECK constraints in tables", arity = 1)
    public boolean testCheckConstraints = true;

    @Parameter(names = {
            "--test-nulls-first-last" }, description = "Allow NULLS FIRST/NULLS LAST in ordering terms", arity = 1)
    public boolean testNullsFirstLast = true;

    @Parameter(names = { "--test-joins" }, description = "Allow the generation of JOIN clauses", arity = 1)
    public boolean testJoins = true;

    @Parameter(names = {
            "--test-functions" }, description = "Allow the generation of functions in expressions", arity = 1)
    public boolean testFunctions = true;

    @Parameter(names = {
            "--test-soundex" }, description = "Test the soundex function, which can be enabled using a compile-time option.", arity = 1)
    public boolean testSoundex;

    @Parameter(names = { "--test-match" }, description = "Allow the generation of the MATCH operator", arity = 1)
    public boolean testMatch = true;

    @Parameter(names = { "--test-in-operator" }, description = "Allow the generation of the IN operator", arity = 1)
    public boolean testIn = true;

    @Parameter(names = {
            "--test-distinct-in-view" }, description = "DISTINCT in views might cause occasional false positives in NoREC and TLP", arity = 1)
    public boolean testDistinctInView;

    @Parameter(names = "--oracle")
    public SQLite3OracleFactory oracles = SQLite3OracleFactory.NoREC;

    @Parameter(names = {
            "--delete-existing-databases" }, description = "Delete a database file if it already exists", arity = 1)
    public boolean deleteIfExists = true;

    @Parameter(names = {
            "--generate-new-database" }, description = "Specifies whether new databases should be generated", arity = 1)
    public boolean generateDatabase = true;

    @Parameter(names = {
            "--execute-queries" }, description = "Specifies whether the query in the fuzzer should be executed", arity = 1)
    public boolean executeQuery = true;

    public enum SQLite3OracleFactory implements OracleFactory<SQLite3GlobalState> {
        PQS {
            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                return new SQLite3PivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        },
        NoREC {
            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                return new SQLite3NoRECOracle(globalState);
            }
        },
        AGGREGATE {

            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                return new SQLite3TLPAggregateOracle(globalState);
            }

        },
        WHERE {

            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                return new SQLite3TLPWhereOracle(globalState);
            }

        },
        DISTINCT {
            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                return new SQLite3TLPDistinctOracle(globalState);
            }
        },
        GROUP_BY {
            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                return new SQLite3TLPGroupByOracle(globalState);
            }
        },
        HAVING {
            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                return new SQLite3TLPHavingOracle(globalState);
            }
        },
        FUZZER {
            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                return new SQLite3Fuzzer(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(SQLite3GlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new SQLite3TLPWhereOracle(globalState));
                oracles.add(new SQLite3TLPDistinctOracle(globalState));
                oracles.add(new SQLite3TLPGroupByOracle(globalState));
                oracles.add(new SQLite3TLPHavingOracle(globalState));
                oracles.add(new SQLite3TLPAggregateOracle(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };

    }

    @Override
    public List<SQLite3OracleFactory> getTestOracleFactory() {
        return Arrays.asList(oracles);
    }

}
