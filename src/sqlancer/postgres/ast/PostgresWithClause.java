package sqlancer.postgres.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.postgres.PostgresSchema.PostgresDataType;


public class PostgresWithClause implements PostgresExpression {
    private final List<PostgresCTE> cteList;

    public PostgresWithClause(List<PostgresCTE> cteList) {
        if (cteList == null || cteList.isEmpty()) {
            throw new IllegalArgumentException("CTE list cannot be null or empty");
        }
        this.cteList = new ArrayList<>(cteList);
    }

    public List<PostgresCTE> getCteList() {
        return new ArrayList<>(cteList);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return null;
    }

  
    public String asString() {
        StringBuilder sb = new StringBuilder();
        sb.append("WITH ");
        
        sb.append(cteList.stream()
                 .map(PostgresCTE::asString)
                 .collect(Collectors.joining(", ")));
        
        return sb.toString();
    }


    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {
        private List<PostgresCTE> cteList = new ArrayList<>();

        public Builder addCTE(PostgresCTE cte) {
            this.cteList.add(cte);
            return this;
        }

        public Builder cteList(List<PostgresCTE> cteList) {
            this.cteList = new ArrayList<>(cteList);
            return this;
        }

        public PostgresWithClause build() {
            return new PostgresWithClause(cteList);
        }
    }
} 