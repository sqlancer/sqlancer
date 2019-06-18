package lama.mysql.ast;

import lama.Randomly;

public abstract class MySQLConstant extends MySQLExpression {
	
	public boolean isInt() {
		return false;
	}
	
	public boolean isNull() {
		return false;
	}
	
	
	public static class MySQLIntConstant extends MySQLConstant {
		
		private final int value;
		private final String stringRepresentation;
		
		public MySQLIntConstant(int value) {
			this.value = value;
			if (value == 0 && Randomly.getBoolean()) {
				stringRepresentation = "FALSE";
			} else if (value == 1 && Randomly.getBoolean()) {
				stringRepresentation = "TRUE";
			} else {
				stringRepresentation = String.valueOf(value);
			}
		}
		
		public MySQLIntConstant(int value, String stringRepresentation) {
			this.value = value;
			this.stringRepresentation = stringRepresentation;
		}
		
		@Override
		public boolean isInt() {
			return true;
		}
		
		@Override
		public int getInt() {
			return value;
		}

		@Override
		public boolean asBooleanNotNull() {
			return value != 0;
		}

		@Override
		public String getTextRepresentation() {
			return stringRepresentation;
		}
		
	}
	
	public static class MySQLNullConstant extends MySQLConstant {
		
		@Override
		public boolean isNull() {
			return true;
		}

		@Override
		public boolean asBooleanNotNull() {
			throw new UnsupportedOperationException(this.toString());
		}

		@Override
		public String getTextRepresentation() {
			return "NULL";
		}
		
	}


	public int getInt() {
		throw new UnsupportedOperationException();
	}


	public static MySQLConstant createNullConstant() {
		return new MySQLNullConstant();
	}


	public static MySQLConstant createIntConstant(int value) {
		return new MySQLIntConstant(value);
	}
	
	public static MySQLConstant createIntConstantNotAsBoolean(int value) {
		return new MySQLIntConstant(value, String.valueOf(value));
	}
	
	@Override
	public MySQLConstant getExpectedValue() {
		return this;
	}
	
	public abstract boolean asBooleanNotNull();
	
	public abstract String getTextRepresentation();

	public static MySQLConstant createFalse() {
		return MySQLConstant.createIntConstant(0);
	}

	public static MySQLConstant createBoolean(boolean isTrue) {
		return MySQLConstant.createIntConstant(isTrue ? 1 : 0);
	}

	public static MySQLConstant createTrue() {
		return MySQLConstant.createIntConstant(1);
	}
	
	@Override
	public String toString() {
		return getTextRepresentation();
	}
	
}
