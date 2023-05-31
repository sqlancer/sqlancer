package sqlancer.materialize;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;

public class MaterializeGlobalState extends SQLGlobalState<MaterializeOptions, MaterializeSchema> {

    public static final char IMMUTABLE = 'i';
    public static final char STABLE = 's';
    public static final char VOLATILE = 'v';

    private List<String> operators = Collections.emptyList();
    private List<String> collates = Collections.emptyList();
    private List<String> opClasses = Collections.emptyList();
    private List<String> tableAccessMethods = Collections.emptyList();
    // store and allow filtering by function volatility classifications
    private final Map<String, Character> functionsAndTypes = new HashMap<>();
    private List<Character> allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);

    @Override
    public void setConnection(SQLConnection con) {
        super.setConnection(con);
        try {
            this.opClasses = getOpclasses();
            this.operators = getOperators(getConnection());
            this.collates = getCollnames(getConnection());
            this.tableAccessMethods = getTableAccessMethods(getConnection());
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private List<String> getCollnames(SQLConnection con) throws SQLException {
        List<String> collNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("SELECT collname FROM pg_collation WHERE collname LIKE '%utf8' or collname = 'C';")) {
                while (rs.next()) {
                    collNames.add(rs.getString(1));
                }
            }
        }
        return collNames;
    }

    private List<String> getOpclasses() throws SQLException {
        List<String> opClasses = new ArrayList<>();
        // select opcname FROM pg_opclass;
        // ERROR: unknown catalog item 'pg_opclass'
        opClasses.add("array_ops");
        opClasses.add("array_ops");
        opClasses.add("bit_ops");
        opClasses.add("bool_ops");
        opClasses.add("bpchar_ops");
        opClasses.add("bpchar_ops");
        opClasses.add("bytea_ops");
        opClasses.add("char_ops");
        opClasses.add("char_ops");
        opClasses.add("cidr_ops");
        opClasses.add("cidr_ops");
        opClasses.add("date_ops");
        opClasses.add("date_ops");
        opClasses.add("float4_ops");
        opClasses.add("float4_ops");
        opClasses.add("float8_ops");
        opClasses.add("float8_ops");
        opClasses.add("inet_ops");
        opClasses.add("inet_ops");
        opClasses.add("inet_ops");
        opClasses.add("inet_ops");
        opClasses.add("int2_ops");
        opClasses.add("int2_ops");
        opClasses.add("int4_ops");
        opClasses.add("int4_ops");
        opClasses.add("int8_ops");
        opClasses.add("int8_ops");
        opClasses.add("interval_ops");
        opClasses.add("interval_ops");
        opClasses.add("macaddr_ops");
        opClasses.add("macaddr_ops");
        opClasses.add("macaddr8_ops");
        opClasses.add("macaddr8_ops");
        opClasses.add("name_ops");
        opClasses.add("name_ops");
        opClasses.add("numeric_ops");
        opClasses.add("numeric_ops");
        opClasses.add("oid_ops");
        opClasses.add("oid_ops");
        opClasses.add("oidvector_ops");
        opClasses.add("oidvector_ops");
        opClasses.add("record_ops");
        opClasses.add("record_image_ops");
        opClasses.add("text_ops");
        opClasses.add("text_ops");
        opClasses.add("time_ops");
        opClasses.add("time_ops");
        opClasses.add("timestamptz_ops");
        opClasses.add("timestamptz_ops");
        opClasses.add("timetz_ops");
        opClasses.add("timetz_ops");
        opClasses.add("varbit_ops");
        opClasses.add("varchar_ops");
        opClasses.add("varchar_ops");
        opClasses.add("timestamp_ops");
        opClasses.add("timestamp_ops");
        opClasses.add("text_pattern_ops");
        opClasses.add("varchar_pattern_ops");
        opClasses.add("bpchar_pattern_ops");
        opClasses.add("money_ops");
        opClasses.add("bool_ops");
        opClasses.add("bytea_ops");
        opClasses.add("tid_ops");
        opClasses.add("xid_ops");
        opClasses.add("cid_ops");
        opClasses.add("tid_ops");
        opClasses.add("text_pattern_ops");
        opClasses.add("varchar_pattern_ops");
        opClasses.add("bpchar_pattern_ops");
        opClasses.add("aclitem_ops");
        opClasses.add("box_ops");
        opClasses.add("point_ops");
        opClasses.add("text_pattern_ops");
        opClasses.add("varchar_pattern_ops");
        opClasses.add("bpchar_pattern_ops");
        opClasses.add("money_ops");
        opClasses.add("bool_ops");
        opClasses.add("bytea_ops");
        opClasses.add("tid_ops");
        opClasses.add("xid_ops");
        opClasses.add("cid_ops");
        opClasses.add("tid_ops");
        opClasses.add("text_pattern_ops");
        opClasses.add("varchar_pattern_ops");
        opClasses.add("bpchar_pattern_ops");
        opClasses.add("aclitem_ops");
        opClasses.add("box_ops");
        opClasses.add("point_ops");
        opClasses.add("poly_ops");
        opClasses.add("circle_ops");
        opClasses.add("array_ops");
        opClasses.add("uuid_ops");
        opClasses.add("uuid_ops");
        opClasses.add("pg_lsn_ops");
        opClasses.add("pg_lsn_ops");
        opClasses.add("enum_ops");
        opClasses.add("enum_ops");
        opClasses.add("tsvector_ops");
        opClasses.add("tsvector_ops");
        opClasses.add("tsvector_ops");
        opClasses.add("tsquery_ops");
        opClasses.add("tsquery_ops");
        opClasses.add("range_ops");
        opClasses.add("range_ops");
        opClasses.add("range_ops");
        opClasses.add("range_ops");
        opClasses.add("box_ops");
        opClasses.add("quad_point_ops");
        opClasses.add("kd_point_ops");
        opClasses.add("text_ops");
        opClasses.add("poly_ops");
        opClasses.add("jsonb_ops");
        opClasses.add("jsonb_ops");
        opClasses.add("jsonb_ops");
        opClasses.add("jsonb_path_ops");
        opClasses.add("bytea_minmax_ops");
        opClasses.add("char_minmax_ops");
        opClasses.add("name_minmax_ops");
        opClasses.add("int8_minmax_ops");
        opClasses.add("int2_minmax_ops");
        opClasses.add("int4_minmax_ops");
        opClasses.add("text_minmax_ops");
        opClasses.add("oid_minmax_ops");
        opClasses.add("tid_minmax_ops");
        opClasses.add("float4_minmax_ops");
        opClasses.add("float8_minmax_ops");
        opClasses.add("macaddr_minmax_ops");
        opClasses.add("macaddr8_minmax_ops");
        opClasses.add("inet_minmax_ops");
        opClasses.add("inet_inclusion_ops");
        opClasses.add("bpchar_minmax_ops");
        opClasses.add("time_minmax_ops");
        opClasses.add("date_minmax_ops");
        opClasses.add("timestamp_minmax_ops");
        opClasses.add("timestamptz_minmax_ops");
        opClasses.add("interval_minmax_ops");
        opClasses.add("timetz_minmax_ops");
        opClasses.add("bit_minmax_ops");
        opClasses.add("varbit_minmax_ops");
        opClasses.add("numeric_minmax_ops");
        opClasses.add("uuid_minmax_ops");
        opClasses.add("range_inclusion_ops");
        opClasses.add("pg_lsn_minmax_ops");
        opClasses.add("box_inclusion_ops");
        return opClasses;
    }

    private List<String> getOperators(SQLConnection con) throws SQLException {
        List<String> operators = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT oprname FROM pg_operator;")) {
                while (rs.next()) {
                    operators.add(rs.getString(1));
                }
            }
        }
        return operators;
    }

    private List<String> getTableAccessMethods(SQLConnection con) throws SQLException {
        List<String> tableAccessMethods = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            /*
             * pg_am includes both index and table access methods so we need to filter with amtype = 't'
             */
            try (ResultSet rs = s.executeQuery("SELECT amname FROM pg_am WHERE amtype = 't';")) {
                while (rs.next()) {
                    tableAccessMethods.add(rs.getString(1));
                }
            }
        }
        return tableAccessMethods;
    }

    public List<String> getOperators() {
        return operators;
    }

    public String getRandomOperator() {
        return Randomly.fromList(operators);
    }

    public List<String> getCollates() {
        return collates;
    }

    public String getRandomCollate() {
        return Randomly.fromList(collates);
    }

    public List<String> getOpClasses() {
        return opClasses;
    }

    public String getRandomOpclass() {
        return Randomly.fromList(opClasses);
    }

    public List<String> getTableAccessMethods() {
        return tableAccessMethods;
    }

    public String getRandomTableAccessMethod() {
        return Randomly.fromList(tableAccessMethods);
    }

    @Override
    public MaterializeSchema readSchema() throws SQLException {
        return MaterializeSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public void addFunctionAndType(String functionName, Character functionType) {
        this.functionsAndTypes.put(functionName, functionType);
    }

    public Map<String, Character> getFunctionsAndTypes() {
        return this.functionsAndTypes;
    }

    public void setAllowedFunctionTypes(List<Character> types) {
        this.allowedFunctionTypes = types;
    }

    public void setDefaultAllowedFunctionTypes() {
        this.allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);
    }

    public List<Character> getAllowedFunctionTypes() {
        return this.allowedFunctionTypes;
    }

}
