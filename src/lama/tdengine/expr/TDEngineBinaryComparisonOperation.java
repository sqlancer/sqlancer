package lama.tdengine.expr;

import lama.Randomly;
import lama.tdengine.TDEngineSchema.TDEngineDataType;

public class TDEngineBinaryComparisonOperation extends TDEngineExpression {
	
	private final TDEngineExpression left;
	private final TDEngineExpression right;
	private final TDBinaryComparisonOperation op;

	public static enum TDBinaryComparisonOperation {
//		LARGER_THAN(">") {
//			@Override
//			public TDEngineConstant apply(TDEngineConstant expectedValue, TDEngineConstant expectedValue2) {
//				throw new AssertionError();
//			}
//		},
//		SMALLER_THAN("<") {
//			@Override
//			public TDEngineConstant apply(TDEngineConstant expectedValue, TDEngineConstant expectedValue2) {
//				throw new AssertionError();
//			}
//		},
//		LARGER_THAN_OR_EQUAL(">=") {
//			@Override
//			public TDEngineConstant apply(TDEngineConstant expectedValue, TDEngineConstant expectedValue2) {
//				throw new AssertionError();
//			}
//		},
//		SMALLER_THAN_OR_EQUAL("<=") {
//			@Override
//			public TDEngineConstant apply(TDEngineConstant expectedValue, TDEngineConstant expectedValue2) {
//				throw new AssertionError();
//			}
//		},
		EQUAL("=") {
			@Override
			public TDEngineConstant apply(TDEngineConstant expectedValue, TDEngineConstant expectedValue2) {
				return expectedValue.isEquals(expectedValue2);
			}
		}
//		,
//		NOT_EQUAL("<>") {
//			@Override
//			public TDEngineConstant apply(TDEngineConstant expectedValue, TDEngineConstant expectedValue2) {
//				throw new AssertionError();
//			}
//		}
		;
		
		private String s;

		TDBinaryComparisonOperation(String s) {
			this.s = s;
		}
	
		@Override
		public String toString() {
			return s;
		}

		public abstract TDEngineConstant apply(TDEngineConstant expectedValue, TDEngineConstant expectedValue2);

		public static TDBinaryComparisonOperation getRandom() {
			return Randomly.fromOptions(values());
		}
	
	};
	
	public TDEngineBinaryComparisonOperation(TDEngineExpression left, TDEngineExpression right, TDBinaryComparisonOperation op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}
	
	public TDEngineExpression getLeft() {
		return left;
	}
	
	public TDEngineExpression getRight() {
		return right;
	}
	
	public TDBinaryComparisonOperation getOp() {
		return op;
	}

	@Override
	public TDEngineConstant getExpectedValue() {
		TDEngineConstant leftVal = left.getExpectedValue();
		TDEngineConstant rightVal = right.getExpectedValue();
		TDEngineDataType type;
		if (left instanceof TDEngineColumnName) {
			type = ((TDEngineColumnName) left).getColumn().getColumnType();
			rightVal = rightVal.castAs(type);
		} else if (right instanceof TDEngineColumnName) {
			type = ((TDEngineColumnName) right).getColumn().getColumnType();
			leftVal = leftVal.castAs(type);
		}
		return op.apply(leftVal, rightVal);
	}
	
}
