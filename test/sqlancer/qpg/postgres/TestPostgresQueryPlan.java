package sqlancer.qpg.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresProvider;

public class TestPostgresQueryPlan {

    @Test
    void testPostgresQueryPlan() throws Exception {
        String databaseName = "postgres";
        PostgresProvider provider = new PostgresProvider();
        PostgresGlobalState state = provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
        PostgresOptions postgresOption = provider.getOptionClass().getDeclaredConstructor().newInstance();
        state.setDbmsSpecificOptions(postgresOption);
        state.setDatabaseName(databaseName);
        MainOptions options = new MainOptions();
        state.setMainOptions(options);
        state.setState(provider.getStateToReproduce(databaseName));
        SQLConnection con = provider.createDatabase(state);
        state.setConnection(con);
        Main.StateLogger logger = new Main.StateLogger(databaseName, provider, options);
        state.setStateLogger(logger);

        SQLQueryAdapter q = new SQLQueryAdapter("CREATE TABLE t1(a INT, b INT);", true);
        q.execute(state);
        q = new SQLQueryAdapter("CREATE TABLE t2(c INT);", true);
        q.execute(state);
        String queryPlan = provider.getQueryPlan("SELECT * FROM t1 RIGHT JOIN t2 ON a<>0;", state);
        assertEquals("Nested Loop Seq Scan Materialize Seq Scan", queryPlan);
    }

    @Test
    void testFormatQueryPlan() throws Exception {

        PostgresProvider provider = new PostgresProvider();

        String queryPlan = "[\n" + "  {\n" + "    \"Plan\": {\n" + "      \"Node Type\": \"Aggregate\",\n"
                + "      \"Strategy\": \"Hashed\",\n" + "      \"Partial Mode\": \"Simple\",\n"
                + "      \"Parallel Aware\": false,\n" + "      \"Async Capable\": false,\n"
                + "      \"Startup Cost\": 62998.82,\n" + "      \"Total Cost\": 63009.32,\n"
                + "      \"Plan Rows\": 1050,\n" + "      \"Plan Width\": 4,\n" + "      \"Output\": [\"t1.c0\"],\n"
                + "      \"Group Key\": [\"t1.c0\"],\n" + "      \"Planned Partitions\": 0,\n" + "      \"Plans\": [\n"
                + "        {\n" + "          \"Node Type\": \"Append\",\n"
                + "          \"Parent Relationship\": \"Outer\",\n" + "          \"Parallel Aware\": false,\n"
                + "          \"Async Capable\": false,\n" + "          \"Startup Cost\": 27150.40,\n"
                + "          \"Total Cost\": 62996.20,\n" + "          \"Plan Rows\": 1050,\n"
                + "          \"Plan Width\": 4,\n" + "          \"Subplans Removed\": 0,\n" + "          \"Plans\": [\n"
                + "            {\n" + "              \"Node Type\": \"Group\",\n"
                + "              \"Parent Relationship\": \"Member\",\n" + "              \"Parallel Aware\": false,\n"
                + "              \"Async Capable\": false,\n" + "              \"Startup Cost\": 27150.40,\n"
                + "              \"Total Cost\": 62949.08,\n" + "              \"Plan Rows\": 200,\n"
                + "              \"Plan Width\": 4,\n" + "              \"Output\": [\"t1.c0\"],\n"
                + "              \"Group Key\": [\"t1.c0\"],\n" + "              \"Plans\": [\n" + "                {\n"
                + "                  \"Node Type\": \"Gather Merge\",\n"
                + "                  \"Parent Relationship\": \"Outer\",\n"
                + "                  \"Parallel Aware\": false,\n" + "                  \"Async Capable\": false,\n"
                + "                  \"Startup Cost\": 27150.40,\n" + "                  \"Total Cost\": 62948.08,\n"
                + "                  \"Plan Rows\": 400,\n" + "                  \"Plan Width\": 4,\n"
                + "                  \"Output\": [\"t1.c0\"],\n" + "                  \"Workers Planned\": 2,\n"
                + "                  \"Plans\": [\n" + "                    {\n"
                + "                      \"Node Type\": \"Group\",\n"
                + "                      \"Parent Relationship\": \"Outer\",\n"
                + "                      \"Parallel Aware\": false,\n"
                + "                      \"Async Capable\": false,\n"
                + "                      \"Startup Cost\": 26150.38,\n"
                + "                      \"Total Cost\": 61901.89,\n" + "                      \"Plan Rows\": 200,\n"
                + "                      \"Plan Width\": 4,\n" + "                      \"Output\": [\"t1.c0\"],\n"
                + "                      \"Group Key\": [\"t1.c0\"],\n" + "                      \"Plans\": [\n"
                + "                        {\n" + "                          \"Node Type\": \"Merge Join\",\n"
                + "                          \"Parent Relationship\": \"Outer\",\n"
                + "                          \"Parallel Aware\": false,\n"
                + "                          \"Async Capable\": false,\n"
                + "                          \"Join Type\": \"Inner\",\n"
                + "                          \"Startup Cost\": 26150.38,\n"
                + "                          \"Total Cost\": 56906.48,\n"
                + "                          \"Plan Rows\": 1998164,\n"
                + "                          \"Plan Width\": 4,\n"
                + "                          \"Output\": [\"t1.c0\"],\n"
                + "                          \"Inner Unique\": false,\n"
                + "                          \"Merge Cond\": \"(t0.c0 = t1.c0)\",\n"
                + "                          \"Plans\": [\n" + "                            {\n"
                + "                              \"Node Type\": \"Sort\",\n"
                + "                              \"Parent Relationship\": \"Outer\",\n"
                + "                              \"Parallel Aware\": false,\n"
                + "                              \"Async Capable\": false,\n"
                + "                              \"Startup Cost\": 25970.60,\n"
                + "                              \"Total Cost\": 26362.39,\n"
                + "                              \"Plan Rows\": 156719,\n"
                + "                              \"Plan Width\": 4,\n"
                + "                              \"Output\": [\"t0.c0\"],\n"
                + "                              \"Sort Key\": [\"t0.c0\"],\n"
                + "                              \"Plans\": [\n" + "                                {\n"
                + "                                  \"Node Type\": \"Seq Scan\",\n"
                + "                                  \"Parent Relationship\": \"Outer\",\n"
                + "                                  \"Parallel Aware\": true,\n"
                + "                                  \"Async Capable\": false,\n"
                + "                                  \"Relation Name\": \"t0\",\n"
                + "                                  \"Schema\": \"public\",\n"
                + "                                  \"Alias\": \"t0\",\n"
                + "                                  \"Startup Cost\": 0.00,\n"
                + "                                  \"Total Cost\": 10301.95,\n"
                + "                                  \"Plan Rows\": 156719,\n"
                + "                                  \"Plan Width\": 4,\n"
                + "                                  \"Output\": [\"t0.c0\"],\n"
                + "                                  \"Filter\": \"(t0.c0 < 100)\"\n"
                + "                                }\n" + "                              ]\n"
                + "                            },\n" + "                            {\n"
                + "                              \"Node Type\": \"Sort\",\n"
                + "                              \"Parent Relationship\": \"Inner\",\n"
                + "                              \"Parallel Aware\": false,\n"
                + "                              \"Async Capable\": false,\n"
                + "                              \"Startup Cost\": 179.78,\n"
                + "                              \"Total Cost\": 186.16,\n"
                + "                              \"Plan Rows\": 2550,\n"
                + "                              \"Plan Width\": 4,\n"
                + "                              \"Output\": [\"t1.c0\"],\n"
                + "                              \"Sort Key\": [\"t1.c0\"],\n"
                + "                              \"Plans\": [\n" + "                                {\n"
                + "                                  \"Node Type\": \"Seq Scan\",\n"
                + "                                  \"Parent Relationship\": \"Outer\",\n"
                + "                                  \"Parallel Aware\": false,\n"
                + "                                  \"Async Capable\": false,\n"
                + "                                  \"Relation Name\": \"t1\",\n"
                + "                                  \"Schema\": \"public\",\n"
                + "                                  \"Alias\": \"t1\",\n"
                + "                                  \"Startup Cost\": 0.00,\n"
                + "                                  \"Total Cost\": 35.50,\n"
                + "                                  \"Plan Rows\": 2550,\n"
                + "                                  \"Plan Width\": 4,\n"
                + "                                  \"Output\": [\"t1.c0\"]\n" + "                                }\n"
                + "                              ]\n" + "                            }\n"
                + "                          ]\n" + "                        }\n" + "                      ]\n"
                + "                    }\n" + "                  ]\n" + "                }\n" + "              ]\n"
                + "            },\n" + "            {\n" + "              \"Node Type\": \"Bitmap Heap Scan\",\n"
                + "              \"Parent Relationship\": \"Member\",\n" + "              \"Parallel Aware\": false,\n"
                + "              \"Async Capable\": false,\n" + "              \"Relation Name\": \"t2\",\n"
                + "              \"Schema\": \"public\",\n" + "              \"Alias\": \"t2\",\n"
                + "              \"Startup Cost\": 10.74,\n" + "              \"Total Cost\": 31.37,\n"
                + "              \"Plan Rows\": 850,\n" + "              \"Plan Width\": 4,\n"
                + "              \"Output\": [\"t2.c0\"],\n" + "              \"Recheck Cond\": \"(t2.c0 < 10)\",\n"
                + "              \"Plans\": [\n" + "                {\n"
                + "                  \"Node Type\": \"Bitmap Index Scan\",\n"
                + "                  \"Parent Relationship\": \"Outer\",\n"
                + "                  \"Parallel Aware\": false,\n" + "                  \"Async Capable\": false,\n"
                + "                  \"Index Name\": \"t2_pkey\",\n" + "                  \"Startup Cost\": 0.00,\n"
                + "                  \"Total Cost\": 10.53,\n" + "                  \"Plan Rows\": 850,\n"
                + "                  \"Plan Width\": 0,\n" + "                  \"Index Cond\": \"(t2.c0 < 10)\"\n"
                + "                }\n" + "              ]\n" + "            }\n" + "          ]\n" + "        }\n"
                + "      ]\n" + "    },\n" + "    \"Planning Time\": 1.954\n" + "  }\n" + "]\n";

        String formatedQueryPlan = provider.formatQueryPlan(queryPlan);
        assertEquals(
                "Aggregate Append Group Bitmap Heap Scan Gather Merge Bitmap Index Scan Group Merge Join Sort Sort Seq Scan Seq Scan",
                formatedQueryPlan);
    }

}
