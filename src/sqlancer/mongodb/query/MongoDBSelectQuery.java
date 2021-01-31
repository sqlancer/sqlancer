package sqlancer.mongodb.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import sqlancer.GlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mongodb.MongoDBConnection;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.ast.MongoDBExpression;
import sqlancer.mongodb.ast.MongoDBSelect;
import sqlancer.mongodb.visitor.MongoDBVisitor;

public class MongoDBSelectQuery extends MongoDBQueryAdapter {

    private final MongoDBSelect<MongoDBExpression> select;

    private List<Document> resultSet;

    public MongoDBSelectQuery(MongoDBSelect<MongoDBExpression> select) {
        this.select = select;
    }

    @Override
    public boolean couldAffectSchema() {
        return false;
    }

    @Override
    public <G extends GlobalState<?, ?, MongoDBConnection>> boolean execute(G globalState, String... fills)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        ExpectedErrors errors = new ExpectedErrors();
        // ARITHMETIC
        errors.add("Failed to optimize pipeline :: caused by :: Can't coerce out of range value");
        errors.add("Can't coerce out of range value");
        errors.add("date overflow in $add");
        errors.add("Failed to optimize pipeline :: caused by :: $sqrt only supports numeric types, not");
        errors.add("Failed to optimize pipeline :: caused by :: $sqrt's argument must be greater than or equal to 0");
        errors.add("Failed to optimize pipeline :: caused by :: $pow's base must be numeric, not");
        errors.add("Failed to optimize pipeline :: caused by :: $pow cannot take a base of 0 and a negative exponent");
        errors.add("Failed to optimize pipeline :: caused by :: $add only supports numeric or date types, not");
        errors.add("Failed to optimize pipeline :: caused by :: $exp only supports numeric types, not");
        errors.add("Failed to optimize pipeline :: caused by :: $log's base must be numeric, not");
        errors.add("Failed to optimize pipeline :: caused by :: $log's base must be a positive number not equal to 1");
        errors.add("Failed to optimize pipeline :: caused by :: $multiply only supports numeric types, not");
        errors.add("$log's argument must be numeric, not");
        errors.add("$log's argument must be a positive number, but");
        errors.add("$log's base must be numeric, not");
        errors.add("$log's base must be a positive number not equal to 1");
        errors.add("$divide only supports numeric types, not");
        errors.add("can't $divide by zero");
        errors.add("$pow's exponent must be numeric, not");
        errors.add("$pow's base must be numeric, not");
        errors.add("$pow cannot take a base of 0 and a negative exponent");
        errors.add("$add only supports numeric or date types, not");
        errors.add("only one date allowed in an $add expression");
        errors.add("$multiply only supports numeric types, not");
        errors.add("$exp only supports numeric types, not");
        errors.add("$sqrt's argument must be greater than or equal to 0");
        errors.add("$sqrt only supports numeric types, not");

        // REGEX
        errors.add("Regular expression is invalid: nothing to repeat");
        errors.add("Regular expression is invalid: missing terminating ] for character class");
        errors.add("Regular expression is invalid: unmatched parentheses");
        errors.add("Regular expression is invalid: missing )");
        errors.add("Regular expression is invalid: invalid UTF-8 string");
        errors.add("Regular expression is invalid: \\k is not followed by a braced, angle-bracketed, or quoted name");
        errors.add("Regular expression is invalid: missing opening brace after \\\\o");
        errors.add("Regular expression is invalid: reference to non-existent subpattern");
        errors.add("Regular expression is invalid: \\ at end of pattern");
        errors.add("Regular expression is invalid: PCRE does not support \\L, \\l, \\N{name}, \\U, or \\u");
        errors.add("Regular expression is invalid: (?R or (?[+-]digits must be followed by )");
        errors.add("Regular expression is invalid: unknown property name after \\P or \\p");
        errors.add("Regular expression is invalid: (*VERB) not recognized or malformed");
        errors.add("Regular expression is invalid: a numbered reference must not be zero");
        errors.add("Regular expression is invalid: unrecognized character after (? or (?-");
        errors.add("Regular expression is invalid: \\c at end of pattern");
        errors.add("Regular expression is invalid: malformed \\P or \\p sequence");
        errors.add("Regular expression is invalid: range out of order in character class");
        errors.add("Regular expression is invalid: group name must start with a non-digit");
        errors.add("Regular expression is invalid: \\c must be followed by an ASCII character");
        errors.add("Regular expression is invalid: subpattern name expected");
        errors.add("Regular expression is invalid: POSIX collating elements are not supported");
        errors.add("Regular expression is invalid: closing ) for (?C expected");
        errors.add("Regular expression is invalid: syntax error in subpattern name (missing terminator)");
        errors.add("Regular expression is invalid: \\\\N is not supported in a class");
        errors.add("Regular expression is invalid: non-octal character in \\o{} (closing brace missing?)");
        errors.add("Regular expression is invalid: non-hex character in \\x{} (closing brace missing?)");
        errors.add(
                "Regular expression is invalid: \\g is not followed by a braced, angle-bracketed, or quoted name/number or by a plain number");
        errors.add("Regular expression is invalid: digits missing in \\x{} or \\o{}");
        errors.add("Regular expression is invalid: malformed number or name after (?(");
        errors.add("Regular expression is invalid: digit expected after (?+");
        errors.add("Regular expression is invalid: assertion expected after (?( or (?(?C)");
        errors.add("Regular expression is invalid: unrecognized character after (?P");

        return errors;
    }

    @Override
    public <G extends GlobalState<?, ?, MongoDBConnection>> SQLancerResultSet executeAndGet(G globalState,
            String... fills) throws Exception {
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(this.getLogString());
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        List<Bson> pipeline = MongoDBVisitor.asQuery(select);

        MongoCollection<Document> collection = globalState.getConnection().getDatabase()
                .getCollection(select.getMainTableName());
        MongoCursor<Document> cursor = collection.aggregate(pipeline).cursor();
        resultSet = new ArrayList<>();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            resultSet.add(document);
        }
        return null;
    }

    @Override
    public String getLogString() {
        return MongoDBVisitor.asStringLog(select);
    }

    public List<Document> getResultSet() {
        return resultSet;
    }

}
