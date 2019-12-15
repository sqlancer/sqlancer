package lama.postgres;

import java.util.List;

import lama.Randomly;

public class PostgresGlobalState {
	
	List<String> operators;
	List<String> collates;
	List<String> opClasses;
	
	public PostgresGlobalState(List<String> opClasses, List<String> operators, List<String> collationNames) {
		this.opClasses = opClasses;
		this.operators = operators;
		this.collates = collationNames;
	}

	public List<String> getOperators() {
		return operators;
	}
	
	public String getRandomOperator() {
		return Randomly.fromList(operators);
	}
	
	public List<String> getCollates() {
		return collates;
	}
	
	public String getRandomCollate() {
		return Randomly.fromList(collates);
	}
	
	public List<String> getOpClasses() {
		return opClasses;
	}
	
	public String getRandomOpclass() {
		return Randomly.fromList(opClasses);
	}
 
}
