package sqlancer.sqlite3;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

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
            "--max-num-tables" }, description = "The maximum number of tables/virtual tables/ rtree tables/ views that can be created")
    public int maxNumTables = 10;

    @Parameter(names = { "--max-num-indexes" }, description = "The maximum number of indexes that can be created")
    public int maxNumIndexes = 20;

    public enum coddtest_model {
        random, expression, subquery;
        public boolean isRandom() {
            return this == random;
        }
        public boolean isExpression() {
            return this == expression;
        }
        public boolean isSubquery() {
            return this == subquery;
        }
    }
    @Parameter(names = { "--coddtest-model" }, description = "Apply CODDTest on expression, subquery, or random")
    public coddtest_model coddTestModel = coddtest_model.random;

    @Override
    public List<SQLite3OracleFactory> getTestOracleFactory() {
        return Arrays.asList(oracles);
    }

}
