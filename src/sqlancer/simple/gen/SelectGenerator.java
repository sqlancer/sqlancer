package sqlancer.simple.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.simple.clause.Clause;
import sqlancer.simple.dialect.Dialect;
import sqlancer.simple.expression.ColumnName;
import sqlancer.simple.expression.Constant;
import sqlancer.simple.expression.Expression;
import sqlancer.simple.expression.TableName;
import sqlancer.simple.statement.Select;
import sqlancer.simple.type.Type;

public class SelectGenerator<T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>>
        implements Generator, TLPGenerator {

    Dialect dialect;
    List<T> tables;
    List<T> usableTables;
    Randomly randomly;
    int maxExpressionDepth;
    int currExpressionDepth;

    public SelectGenerator(Dialect dialect, List<T> tables, Randomly randomly, int maxExpressionDepth) {
        this.dialect = dialect;
        this.tables = tables;
        this.usableTables = new ArrayList<>();
        this.randomly = randomly;
        this.maxExpressionDepth = maxExpressionDepth;
        this.currExpressionDepth = 0;
    }

    @Override
    public void reset() {
        this.currExpressionDepth = 0;
        this.usableTables = new ArrayList<>();
    }

    @Override
    public Expression generateExpression() {
        if (currExpressionDepth >= maxExpressionDepth || Randomly.getBoolean()) {
            --currExpressionDepth;
            return Randomly.getBoolean() ? generateConstant(getRandomType()) : generateColumn();
        }

        List<Class<? extends Expression>> legalExpressions = dialect.getLegalExpressions();
        assert !legalExpressions.isEmpty() : "generateExpression(): dialect does not have any legal expressions";

        Class<? extends Expression> legalExpression = Randomly.fromList(legalExpressions);

        currExpressionDepth++;
        Expression expression = Expression.construct(legalExpression, this);
        currExpressionDepth--;

        return expression;
    }

    public List<Expression> generateExpressions() {
        int nr = Randomly.smallNumber() + 1;
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression());
        }

        return expressions;
    }

    public Type getRandomType() {
        List<Type> types = dialect.getTypes();
        assert !types.isEmpty() : "getRandomType(): dialect does not have any types";

        return Randomly.fromList(types);
    }

    public Expression generateColumn() {
        assert !usableTables.isEmpty();

        AbstractTable<?, ?, ?> table = Randomly.fromList(usableTables);
        AbstractTableColumn<?, ?> column = Randomly.fromList(table.getColumns());

        return new ColumnName(column.getName(), table.getName());
    }

    public List<Expression> generateColumns() {
        assert !usableTables.isEmpty();

        List<Expression> columnNames = usableTables.stream()
                .flatMap(t -> t.getColumns().stream().map(c -> new ColumnName(c.getName(), t.getName())))
                .collect(Collectors.toList());

        return Randomly.nonEmptySubset(columnNames);
    }

    public Expression generateConstant(Type type) {
        String value = type.instantiateRandomValue(randomly);

        return new Constant(value);
    }

    public Expression generateTableName() {
        int tableId = usableTables.size();
        if (tableId >= tables.size()) {
            return null;
        }
        usableTables.add(tables.get(tableId));

        return new TableName(tables.get(tableId).getName());
    }

    public List<Expression> generateTableNames() {
        long resultSize = randomly.getInteger(1, Math.max(1, tables.size() - 1));
        List<Expression> result = usableTables.stream().map(t -> new TableName(t.getName()))
                .collect(Collectors.toList());
        while (result.size() < resultSize) {
            result.add(generateTableName());
        }

        return result;
    }

    @Override
    public Clause generateClauseOfAny(List<Class<? extends Clause>> clauses) {
        Class<? extends Clause> legalClause = Randomly.fromList(clauses);

        return Clause.construct(legalClause, this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> E generateResponse(Signal signal) {
        switch (signal) {
        case COLUMN_NAME:
            return (E) generateColumn();
        case COLUMN_NAME_LIST:
            return (E) generateColumns();
        case TABLE_NAME:
            return (E) generateTableName();
        case TABLE_NAME_LIST:
            return (E) generateTableNames();
        case CONSTANT_VALUE:
            Type type = getRandomType();
            return (E) generateConstant(type);
        case TYPE_NAME:
            return (E) getRandomType().toString();
        case EXPRESSION:
            return (E) generateExpression();
        case EXPRESSION_LIST:
            return (E) generateExpressions();

        default:
            throw new IllegalArgumentException(signal + " not handled in generateFetchClause()");
        }
    }

    @Override
    public Select generateSelect() {
        return dialect.generateSelect(this);
    }
}
