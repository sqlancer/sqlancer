package sqlancer.sqlite3.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3TableReference;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public final class SQLite3Common {

    private SQLite3Common() {
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions(" COLLATE BINARY", " COLLATE RTRIM", " COLLATE NOCASE"/* , " COLLATE UINT" */);
    }

    public static String getCheckConstraint(SQLite3GlobalState globalState, List<SQLite3Column> columns) {
        SQLite3Expression expression = new SQLite3ExpressionGenerator(globalState).setColumns(columns)
                .generateExpression();
        return " CHECK ( " + SQLite3Visitor.asString(expression) + ")";
    }

    // TODO: refactor others to use this method
    // https://www.sqlite.org/syntax/ordering-term.html
    public static String getOrderingTerm(List<SQLite3Column> columns, SQLite3GlobalState globalState) {
        SQLite3Expression randExpr = new SQLite3ExpressionGenerator(globalState).setColumns(columns)
                .generateExpression();
        StringBuilder sb = new StringBuilder(SQLite3Visitor.asString(randExpr));
        sb.append(" ");
        if (Randomly.getBoolean()) {
            sb.append(SQLite3Common.getRandomCollate());
        }
        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                sb.append(" ASC");
            } else {
                sb.append(" DESC");
            }
        }
        return sb.toString();
    }

    public static String getIndexedClause(String indexName) {
        StringBuilder sb = new StringBuilder();
        if (Randomly.getBoolean()) {
            sb.append("INDEXED BY ");
            sb.append(indexName);
        } else {
            sb.append("NOT INDEXED");
        }
        return sb.toString();
    }

    public static String getFreeTableName(SQLite3Schema s) {
        int nr = 0;
        String[] name = new String[1];
        do {
            name[0] = DBMSCommon.createTableName(nr++);
        } while (s.getDatabaseTables().stream().anyMatch(tab -> tab.getName().contentEquals(name[0])));
        return name[0];
    }

    public static String getFreeViewName(SQLite3Schema s) {
        int nr = 0;
        String[] name = new String[1];
        do {
            name[0] = "v" + nr++;
        } while (s.getDatabaseTables().stream().anyMatch(tab -> tab.getName().contentEquals(name[0])));
        return name[0];
    }

    public static String getFreeIndexName(SQLite3Schema s) {
        List<String> indexNames = s.getIndexNames();
        String candidateName;
        do {
            candidateName = DBMSCommon.createIndexName((int) Randomly.getNotCachedInteger(0, 100));
        } while (indexNames.contains(candidateName));
        return candidateName;
    }

    public static String getFreeColumnName(SQLite3Table t) {
        List<SQLite3Column> indexNames = t.getColumns();
        final String[] candidateName = new String[1];
        do {
            candidateName[0] = DBMSCommon.createColumnName((int) Randomly.getNotCachedInteger(0, 100));
        } while (indexNames.stream().anyMatch(c -> c.getName().contentEquals(candidateName[0])));
        return candidateName[0];
    }

    public static String getOrderByAsString(List<SQLite3Column> columns, SQLite3GlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(globalState).setColumns(columns);
        sb.append(" ORDER BY ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(SQLite3Visitor.asString(gen.generateOrderingTerm()));
        }
        return sb.toString();
    }

    public static List<SQLite3Expression> getOrderBy(List<SQLite3Column> columns, SQLite3GlobalState globalState) {
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(globalState).setColumns(columns);
        List<SQLite3Expression> list = new ArrayList<>();
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            list.add(gen.generateOrderingTerm());
        }
        return list;

    }

    public static SQLite3Column createColumn(int i) {
        return new SQLite3Column(DBMSCommon.createColumnName(i), SQLite3DataType.NONE, false, false, null);
    }

    public static List<SQLite3Expression> getTableRefs(List<SQLite3Table> tables, SQLite3Schema s) {
        List<SQLite3Expression> tableRefs = new ArrayList<>();
        for (SQLite3Table t : tables) {
            SQLite3TableReference tableRef;
            if (Randomly.getBooleanWithSmallProbability() && !s.getIndexNames().isEmpty()) {
                tableRef = new SQLite3TableReference(s.getRandomIndexOrBailout(), t);
            } else {
                tableRef = new SQLite3TableReference(t);
            }
            tableRefs.add(tableRef);
        }
        return tableRefs;
    }

}
