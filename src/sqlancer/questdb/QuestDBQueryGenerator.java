package sqlancer.questdb;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import java.util.List;

public class QuestDBQueryGenerator {

    // Generate a simple SELECT query for QuestDB
    public SQLQueryAdapter generateSelect(String tableName, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        ExpectedErrors errors = new ExpectedErrors();
        
        sb.append("SELECT ");
        if (columns == null || columns.isEmpty()) {
            sb.append("*");
        } else {
            for (int i = 0; i < columns.size(); i++) {
                sb.append(columns.get(i));
                if (i < columns.size() - 1) {
                    sb.append(", ");
                }
            }
        }
        sb.append(" FROM ").append(tableName);
        sb.append(" LIMIT 10"); // simple query without semicolon for better compatibility
        
        // Add QuestDB-specific errors
        QuestDBErrors.addExpressionErrors(errors);
        
        return new SQLQueryAdapter(sb.toString(), errors);
    }
    
    // Generate a simple SELECT query with WHERE clause
    public SQLQueryAdapter generateSelectWithWhere(String tableName, List<String> columns, String whereClause) {
        StringBuilder sb = new StringBuilder();
        ExpectedErrors errors = new ExpectedErrors();
        
        sb.append("SELECT ");
        if (columns == null || columns.isEmpty()) {
            sb.append("*");
        } else {
            for (int i = 0; i < columns.size(); i++) {
                sb.append(columns.get(i));
                if (i < columns.size() - 1) {
                    sb.append(", ");
                }
            }
        }
        sb.append(" FROM ").append(tableName);
        
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        
        sb.append(" LIMIT 10");
        
        // Add QuestDB-specific errors
        QuestDBErrors.addExpressionErrors(errors);
        
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}