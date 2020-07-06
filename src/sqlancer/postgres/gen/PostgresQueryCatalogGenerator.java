package sqlancer.postgres.gen;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import sqlancer.GlobalState;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;

public final class PostgresQueryCatalogGenerator {

    private PostgresQueryCatalogGenerator() {
    }

    public static Query query() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ");
        sb.append(
                Randomly.fromOptions("pg_aggregate", "pg_am", "pg_amop", "pg_amproc", "pg_attrdef", "pg_attribute",
                        "pg_authid", "pg_auth_members", "pg_cast", "pg_class", "pg_collation", "pg_constraint",
                        "pg_conversion", "pg_database", "pg_db_role_setting", "pg_default_acl", "pg_depend",
                        "pg_description", "pg_enum", "pg_event_trigger", "pg_extension", "pg_foreign_data_wrapper",
                        "pg_foreign_server", "pg_foreign_table", "pg_index", "pg_inherits", "pg_init_privs",
                        "pg_language", "pg_largeobject", "pg_largeobject_metadata", "pg_namespace", "pg_opclass",
                        "pg_operator", "pg_opfamily", "pg_partitioned_table", /* "pg_pltemplate", */
                        "pg_policy", "pg_proc", "pg_publication", "pg_publication_rel", "pg_range",
                        "pg_replication_origin", "pg_rewrite", "pg_seclabel", "pg_sequence", "pg_shdepend",
                        "pg_shdescription", "pg_shseclabel", "pg_statistic", "pg_statistic_ext",
                        "pg_statistic_ext_data", "pg_subscription", "pg_subscription_rel", "pg_tablespace",
                        "pg_transform", "pg_trigger", "pg_ts_config", "pg_ts_config_map", "pg_ts_dict", "pg_ts_parser",
                        "pg_ts_template", "pg_type", "pg_user_mapping"));
        return new QueryAdapter(sb.toString()) {
            @Override
            public boolean execute(GlobalState<?> globalState) throws SQLException {
                try (Statement s = globalState.getConnection().createStatement()) {
                    try (ResultSet rs = s.executeQuery(getQueryString())) {
                        // CHECKSTYLE:OFF
                        while (rs.next()) {
                            // only force the DBMS to fetch the records and hope that they cause an
                            // invalid state
                        }
                        // CHECKSTYLE:ON
                    }
                }
                return true;
            }
        };
    }

}
