package lama.cockroachdb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lama.Randomly;
import lama.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import lama.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import lama.cockroachdb.ast.CockroachDBExpression;
import lama.cockroachdb.ast.CockroachDBFunctionCall;

public enum CockroachDBFunction {
	
	
//	GREATEST(null, true) {
//		
//		
//		@Override
//		public boolean isCompatibleWithReturnType(CockroachDBDataType returnType) {
//			return true;
//		}
//		
//		@Override
//		public CockroachDBDataType[] getArgumentTypes(CockroachDBDataType returnType) {
//			int args = Randomly.smallNumber() + 1;
//			CockroachDBDataType[] types = new CockroachDBDataType[args];
//			for (int i = 0; i < types.length; i++) {
//				types[i] = returnType;
//			}
//			return types;
//		}
//	},
	
	IF(null) {
		@Override
		public boolean isCompatibleWithReturnType(CockroachDBCompositeDataType returnType) {
			return true;
		}
		
		@Override
		public CockroachDBDataType[] getArgumentTypes(CockroachDBCompositeDataType returnType) {
			return new CockroachDBDataType[] {CockroachDBDataType.BOOL, returnType.getPrimitiveDataType(), returnType.getPrimitiveDataType()};
		}
	},
	NULLIF(null) {
		@Override
		public boolean isCompatibleWithReturnType(CockroachDBCompositeDataType returnType) {
			return true;
		}
		
		@Override
		public CockroachDBDataType[] getArgumentTypes(CockroachDBCompositeDataType returnType) {
			return new CockroachDBDataType[] {returnType.getPrimitiveDataType(), returnType.getPrimitiveDataType()};
		}
	},

	SUBSTRING(CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.INT),
	
	// MATH and numeric functions
	ABS_INT("ABS", CockroachDBDataType.INT, CockroachDBDataType.INT),
//	ABS_FLOAT("ABS", CockroachDBDataType.INT, CockroachDBDataType.FLOAT),
	ACOS(CockroachDBDataType.FLOAT, CockroachDBDataType.FLOAT),

	// string and byte functions
	ASCII(CockroachDBDataType.INT, CockroachDBDataType.STRING), // ascii(val: string) → int
	BIT_LENGTH1("BIT_LENGTH", CockroachDBDataType.INT, CockroachDBDataType.BYTES), // bit_length(val: bytes) → int
	BIT_LENGTH2("BIT_LENGTH", CockroachDBDataType.INT, CockroachDBDataType.STRING), // bit_length(val: string) → int
	BTRIM1("BTRIM", CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.STRING), // btrim(input: string, trim_chars: string) → string	
	BTRIM2("BTRIM", CockroachDBDataType.STRING, CockroachDBDataType.STRING), // btrim(val: string) → string	
	CHAR_LENGTH1("CHAR_LENGTH", CockroachDBDataType.INT, CockroachDBDataType.BYTES), // char_length(val: bytes) → int	
	CHAR_LENGTH2("CHAR_LENGTH", CockroachDBDataType.INT, CockroachDBDataType.STRING),
	CHARACTER_LENGTH1("CHARACTER_LENGTH", CockroachDBDataType.INT, CockroachDBDataType.STRING),
	CHARACTER_LENGTH2("CHARACTER_LENGTH", CockroachDBDataType.INT, CockroachDBDataType.BYTES),
	CHR(CockroachDBDataType.STRING, CockroachDBDataType.INT),
	INITCAP(CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	LEFT1("LEFT", CockroachDBDataType.BYTES, CockroachDBDataType.BYTES, CockroachDBDataType.INT),
	LEFT2("LEFT", CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.INT),
	LENGTH1("LENGTH", CockroachDBDataType.INT, CockroachDBDataType.BYTES),
	LENGTH2("LENGTH", CockroachDBDataType.INT, CockroachDBDataType.STRING),
	LOWER(CockroachDBDataType.STRING, CockroachDBDataType.STRING),
//	LPAD(CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.INT), // TODO: can cause out of memory errors
	LTRIM(CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	OVERLAY(CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.INT),
	QUOTE_IDENT(CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	QUOTE_LITERAL(CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	QUOTE_NULLABLE(CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	REVERSE(CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	STRPOS(CockroachDBDataType.INT, CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	/* https://github.com/cockroachdb/cockroach/issues/44152 */
	TO_ENGLISH(CockroachDBDataType.STRING, CockroachDBDataType.INT), 
	TO_HEX(CockroachDBDataType.STRING, CockroachDBDataType.INT),
	UPPER(CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	REGEXP_REPLACE(CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.STRING, CockroachDBDataType.STRING),
	
	MD5(CockroachDBDataType.STRING) {
		@Override
				public CockroachDBDataType[] getArgumentTypes(CockroachDBCompositeDataType returnType) {
					int nrArgs = Randomly.smallNumber() + 1;
					CockroachDBDataType[] argTypes = new CockroachDBDataType[nrArgs];
					for (int i = 0; i < nrArgs; i++) {
						argTypes[i] = CockroachDBDataType.STRING;
					}
					return argTypes;
				}
	},
	// System info function
	/* see https://github.com/cockroachdb/cockroach/issues/44203 */
	CURRENT_DATABASE(CockroachDBDataType.STRING),
	CURRENT_SCHEMA(CockroachDBDataType.STRING),
	CURRENT_USER(CockroachDBDataType.STRING),
	VERSION(CockroachDBDataType.STRING),


	;
	
	private CockroachDBDataType returnType;
	private CockroachDBDataType[] argumentTypes;
	private boolean isVariadic;
	private String functionName;

	private CockroachDBFunction(CockroachDBDataType returnType, CockroachDBDataType... argumentTypes) {
		this.returnType = returnType;
		this.argumentTypes = argumentTypes;
		this.functionName = toString();
	}
	
	private CockroachDBFunction(CockroachDBDataType returnType, boolean isVariadic) {
		this.returnType = returnType;
		this.isVariadic = isVariadic;
		this.functionName = toString();
	}
	
	public String getFunctionName() {
		return functionName;
	}
	
	private CockroachDBFunction(String functionName, CockroachDBDataType returnType, CockroachDBDataType... argumentTypes) {
		this.functionName = functionName;
		this.returnType = returnType;
		this.argumentTypes = argumentTypes;
	}
	
	public boolean isCompatibleWithReturnType(CockroachDBCompositeDataType returnType) {
		return this.returnType == returnType.getPrimitiveDataType();
	}
	
	public CockroachDBDataType[] getArgumentTypes(CockroachDBCompositeDataType returnType) {
		return argumentTypes;
	}
	
	public CockroachDBFunctionCall getCall(CockroachDBCompositeDataType returnType, CockroachDBExpressionGenerator gen, int depth) {
		CockroachDBDataType[] argumentTypes2 = getArgumentTypes(returnType);
		List<CockroachDBExpression> arguments = getArgumentsForReturnType(gen, depth, argumentTypes2, returnType);
		CockroachDBFunctionCall call = new CockroachDBFunctionCall(this, arguments);
		return call;
	}

	private List<CockroachDBExpression> getArgumentsForReturnType(CockroachDBExpressionGenerator gen, int depth, CockroachDBDataType[] argumentTypes2, CockroachDBCompositeDataType returnType2) {
		List<CockroachDBExpression> arguments = new ArrayList<>();
		for (int i = 0; i < argumentTypes2.length; i++) {
			arguments.add(gen.generateExpression(argumentTypes2[i].get(), depth + 1));
		}
		return arguments;
	}
	
	public static List<CockroachDBFunction> getFunctionsCompatibleWith(CockroachDBCompositeDataType returnType) {
		return Stream.of(values()).filter(f -> f.isCompatibleWithReturnType(returnType)).collect(Collectors.toList());
	}
	
}
