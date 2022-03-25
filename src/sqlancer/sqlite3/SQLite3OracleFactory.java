package sqlancer.sqlite3;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.sqlite3.oracle.SQLite3Fuzzer;
import sqlancer.sqlite3.oracle.SQLite3NoRECOracle;
import sqlancer.sqlite3.oracle.SQLite3PivotedQuerySynthesisOracle;
import sqlancer.sqlite3.oracle.tlp.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Bijitashya on 03, 2022
 */
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

