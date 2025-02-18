package sqlancer.common.ast;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Expression;

public abstract class JoinBase<T extends Expression<?>> {
    public final T tableReference;
    public T onClause;
    public final JoinType type;

    protected JoinBase(T tableReference, T onClause, JoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public T getTableReference() {
        return tableReference;
    }

    public T getOnClause() {
        return onClause;
    }

    public void setOnClause(T onClause) {
        this.onClause = onClause;
    }

    public JoinType getType() {
        return type;
    }

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL, CROSS, JoinType;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static JoinType getRandomExcept(JoinType... exclude) {
            List<JoinType> available = Arrays.stream(values()).filter(type -> !Arrays.asList(exclude).contains(type))
                    .collect(Collectors.toList());
            return Randomly.fromList(available);
        }

    }
}

