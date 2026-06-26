package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresCTE implements PostgresExpression {
    private final String name;
    private final PostgresSelect subquery;

    public PostgresCTE(String name, PostgresSelect subquery) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("CTE name cannot be null or empty");
        }
        if (subquery == null) {
            throw new IllegalArgumentException("CTE subquery cannot be null");
        }
        this.name = name.trim();
        this.subquery = subquery;
    }

    public String getName() {
        return name;
    }

    public PostgresSelect getSubquery() {
        return subquery;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return null;
    }

    public String asString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(" AS (");
        sb.append(subquery.asString());
        sb.append(")");
        return sb.toString();
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {
        private String name;
        private PostgresSelect subquery;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder subquery(PostgresSelect subquery) {
            this.subquery = subquery;
            return this;
        }

        public PostgresCTE build() {
            return new PostgresCTE(name, subquery);
        }
    }
} 