package sqlancer.tdengine.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import sqlancer.Randomly;
import sqlancer.tdengine.TDEngineProvider;
import sqlancer.tdengine.TDEngineSchema.TDEngineColumn;
import sqlancer.tdengine.TDEngineSchema.TDEngineDataType;
import sqlancer.tdengine.TDEngineSchema.TDEngineRowValue;
import sqlancer.tdengine.expr.TDEngineBinaryComparisonOperation;
import sqlancer.tdengine.expr.TDEngineBinaryComparisonOperation.TDBinaryComparisonOperation;
import sqlancer.tdengine.expr.TDEngineColumnName;
import sqlancer.tdengine.expr.TDEngineConstant;
import sqlancer.tdengine.expr.TDEngineExpression;

public class TDEngineExpressionGenerator {

	private Randomly r;
	private List<TDEngineColumn> columns = new ArrayList<>();
	private TDEngineRowValue rw;
	private boolean usedInWhere;

	public TDEngineExpressionGenerator(Randomly r) {
		this.r = r;
	}

	public TDEngineExpressionGenerator setColumns(List<TDEngineColumn> columns) {
		this.columns = columns;
		return this;
	}

	enum Action {
		BINARY_OPERATOR, LEAF
	}

	public TDEngineExpression getRandomExpression() {
		TDEngineExpression randomExpression = getRandomExpression(0);
		return randomExpression;
	}

	public TDEngineExpression getRandomExpression(int depth) {
		if (depth > TDEngineProvider.EXPRESSION_MAX_DEPTH) {
			return getLeaf();
		}
		List<Action> actions = new ArrayList<>(Arrays.asList(Action.values()));
		if (depth == 0) {
			actions.remove(Action.LEAF);
		}
		Action a = Randomly.fromList(actions);
		switch (a) {
		case BINARY_OPERATOR:
			return getRandomBinaryOperator(depth);
		case LEAF:
			return getLeaf();
		default:
			throw new AssertionError();
		}
	}

	private TDEngineExpression getLeaf() {
		if (Randomly.getBoolean() || columns.size() == 0) {
			return getRandomLiteral();
		} else {
			TDEngineColumn c = Randomly.fromList(columns);
			assert rw != null;
			Map<TDEngineColumn, TDEngineConstant> values = rw.getValues();
			return new TDEngineColumnName(c, values.get(c));
		}
	}

	private TDEngineExpression getRandomLiteral() {
		if (!usedInWhere && Randomly.getBoolean()) {
			return TDEngineConstant.createNull();
		}
		TDEngineDataType type = TDEngineDataType.getRandomType();
		switch (type) {
		case BOOL:
			return TDEngineConstant.createRandomBoolConstant();
		case DOUBLE:
		case FLOAT:
			return TDEngineConstant.createRandomDoubleConstant(r);
		case INT:
			return TDEngineConstant.createRandomIntConstant(r);
		case TEXT:
			return TDEngineConstant.createRandomTextConstant(r);
		case TIMESTAMP:
			return TDEngineConstant.createRandomTimestamp(r);
		default:
			throw new AssertionError(type);
		}
	}

	private TDEngineExpression getRandomBinaryOperator(int depth) {
		TDEngineExpression expr1;
		TDEngineExpression expr2;
		do {
			expr1 = getRandomExpression(depth + 1);
			expr2 = getRandomExpression(depth + 1);
		} while (!isOkay(expr1, expr2));
		return new TDEngineBinaryComparisonOperation(expr1, expr2, TDBinaryComparisonOperation.getRandom());
	}

	private boolean isOkay(TDEngineExpression expr1, TDEngineExpression expr2) {
		if (expr1 instanceof TDEngineConstant && expr2 instanceof TDEngineConstant) {
			return false;
		}
		if (expr2 instanceof TDEngineColumnName && expr2 instanceof TDEngineColumnName) {
			return false;
		}
		return true;
	}

	public TDEngineExpressionGenerator setRowValue(TDEngineRowValue rw) {
		this.rw = rw;
		return this;
	}
	
	public TDEngineExpressionGenerator setUsedInWhere(boolean usedInWhere) {
		this.usedInWhere = usedInWhere;
		return this;
	}

}
