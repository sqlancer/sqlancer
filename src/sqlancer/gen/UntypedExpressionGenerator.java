package sqlancer.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;

public abstract class UntypedExpressionGenerator<E, C> {
	
	protected List<C> columns;
	protected boolean allowAggregates;
	
	public abstract E generateExpression();
	
	public abstract E generateConstant();
	
	protected abstract E generateColumn();
	
	@SuppressWarnings("unchecked") // unsafe
	public <U extends UntypedExpressionGenerator<E, C>> U setColumns(List<C> columns) {
		this.columns = columns;
		return (U) this;
	}
	
	public E generateLeafNode() {
		if (Randomly.getBoolean() && !columns.isEmpty()) {
			return generateColumn();
		} else {
			return generateConstant();
		}
	}
	
	public List<E> generateExpressions(int nr) {
		List<E> expressions = new ArrayList<>();
		for (int i = 0; i < nr; i++) {
			expressions.add(generateExpression());
		}
		return expressions;
	}

	// override this class to also generate ASC, DESC
	public List<E> generateOrderBys() {
		return generateExpressions(Randomly.smallNumber() + 1);
	}

	// override this class to generate aggregate functions
	public E generateHavingClause() {
		allowAggregates = true;
		E expr = generateExpression();
		allowAggregates = false;
		return expr;
	}
	
}
