package sqlancer.yugabyte.ysql.ast;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLJSONBFunction implements YSQLExpression {

    public enum JSONBFunction {
        // Conversion functions
        TO_JSON("to_json", 1, YSQLDataType.JSON),
        TO_JSONB("to_jsonb", 1, YSQLDataType.JSONB),
        
        // Array functions
        JSON_ARRAY_LENGTH("json_array_length", 1, YSQLDataType.INT),
        JSONB_ARRAY_LENGTH("jsonb_array_length", 1, YSQLDataType.INT),
        // JSON_ARRAY_ELEMENTS("json_array_elements", 1, YSQLDataType.JSON),
        // JSONB_ARRAY_ELEMENTS("jsonb_array_elements", 1, YSQLDataType.JSONB),
        // JSON_ARRAY_ELEMENTS_TEXT("json_array_elements_text", 1, YSQLDataType.TEXT),
        // JSONB_ARRAY_ELEMENTS_TEXT("jsonb_array_elements_text", 1, YSQLDataType.TEXT),
        
        // Object functions
        // JSON_OBJECT_KEYS("json_object_keys", 1, YSQLDataType.TEXT),
        // JSONB_OBJECT_KEYS("jsonb_object_keys", 1, YSQLDataType.TEXT),
        // JSON_EACH("json_each", 1, YSQLDataType.TEXT), // returns record
        // JSONB_EACH("jsonb_each", 1, YSQLDataType.TEXT), // returns record
        // JSON_EACH_TEXT("json_each_text", 1, YSQLDataType.TEXT), // returns record
        // JSONB_EACH_TEXT("jsonb_each_text", 1, YSQLDataType.TEXT), // returns record
        
        // Type checking
        JSON_TYPEOF("json_typeof", 1, YSQLDataType.TEXT),
        JSONB_TYPEOF("jsonb_typeof", 1, YSQLDataType.TEXT),
        
        // Building functions
        JSON_BUILD_ARRAY("json_build_array", -1, YSQLDataType.JSON), // variadic
        JSONB_BUILD_ARRAY("jsonb_build_array", -1, YSQLDataType.JSONB), // variadic
        JSON_BUILD_OBJECT("json_build_object", -1, YSQLDataType.JSON), // variadic pairs
        JSONB_BUILD_OBJECT("jsonb_build_object", -1, YSQLDataType.JSONB), // variadic pairs
        
        // Aggregate functions
        JSON_AGG("json_agg", 1, YSQLDataType.JSON),
        JSONB_AGG("jsonb_agg", 1, YSQLDataType.JSONB),
        JSON_OBJECT_AGG("json_object_agg", 2, YSQLDataType.JSON),
        JSONB_OBJECT_AGG("jsonb_object_agg", 2, YSQLDataType.JSONB),
        
        // Path operations
        JSON_EXTRACT_PATH("json_extract_path", -1, YSQLDataType.JSON), // variadic
        JSONB_EXTRACT_PATH("jsonb_extract_path", -1, YSQLDataType.JSONB), // variadic
        JSON_EXTRACT_PATH_TEXT("json_extract_path_text", -1, YSQLDataType.TEXT), // variadic
        JSONB_EXTRACT_PATH_TEXT("jsonb_extract_path_text", -1, YSQLDataType.TEXT), // variadic
        
        // Modification functions (JSONB only)
        JSONB_SET("jsonb_set", 3, YSQLDataType.JSONB), // target, path, new_value
        JSONB_INSERT("jsonb_insert", 3, YSQLDataType.JSONB), // target, path, new_value
        JSONB_PRETTY("jsonb_pretty", 1, YSQLDataType.TEXT),
        
        // Strip functions
        JSON_STRIP_NULLS("json_strip_nulls", 1, YSQLDataType.JSON),
        JSONB_STRIP_NULLS("jsonb_strip_nulls", 1, YSQLDataType.JSONB),
        
        // Path query functions (PostgreSQL 12+)
        JSONB_PATH_EXISTS("jsonb_path_exists", 2, YSQLDataType.BOOLEAN), // jsonb, path
        JSONB_PATH_MATCH("jsonb_path_match", 2, YSQLDataType.BOOLEAN), // jsonb, path
        // JSONB_PATH_QUERY("jsonb_path_query", 2, YSQLDataType.JSONB), // jsonb, path - set-returning function
        JSONB_PATH_QUERY_ARRAY("jsonb_path_query_array", 2, YSQLDataType.JSONB), // jsonb, path
        JSONB_PATH_QUERY_FIRST("jsonb_path_query_first", 2, YSQLDataType.JSONB); // jsonb, path
        
        private final String functionName;
        private final int arity; // -1 for variadic
        private final YSQLDataType returnType;
        
        JSONBFunction(String functionName, int arity, YSQLDataType returnType) {
            this.functionName = functionName;
            this.arity = arity;
            this.returnType = returnType;
        }
        
        public String getFunctionName() {
            return functionName;
        }
        
        public int getArity() {
            return arity;
        }
        
        public YSQLDataType getReturnType() {
            return returnType;
        }
        
        public boolean isVariadic() {
            return arity == -1;
        }
        
        public boolean isAggregate() {
            return this == JSON_AGG || this == JSONB_AGG || 
                   this == JSON_OBJECT_AGG || this == JSONB_OBJECT_AGG;
        }
        
        public static JSONBFunction getRandom() {
            return Randomly.fromOptions(values());
        }
        
        public static JSONBFunction getRandomNonAggregate() {
            JSONBFunction func;
            do {
                func = getRandom();
            } while (func.isAggregate() || func.isSetReturning());
            return func;
        }
        
        public boolean isSetReturning() {
            // These functions return sets and cannot be used in regular expressions
            // All set-returning functions have been commented out
            return false;
        }
    }
    
    private final JSONBFunction function;
    private final List<YSQLExpression> args;
    
    public YSQLJSONBFunction(JSONBFunction function, List<YSQLExpression> args) {
        this.function = function;
        this.args = args;
    }
    
    @Override
    public YSQLDataType getExpressionType() {
        return function.getReturnType();
    }
    
    public JSONBFunction getJSONBFunction() {
        return function;
    }
    
    public String getFunctionName() {
        return function.getFunctionName();
    }
    
    public List<YSQLExpression> getArguments() {
        return args;
    }
}