package ch.epfl.ad.db.parsing;

import java.util.Arrays;
import java.util.List;

public class Qualifier {
	
	private Operator operator;
	private List<Operand> operands;
	
	public Qualifier(Operator operator, final Operand operand) {
		this(operator, Arrays.asList(operand));
	}
	
	public Qualifier(Operator operator, List<Operand> operands) {
		if (operator == null) {
			throw new IllegalArgumentException("Qualifier operator cannot be null.");
		}
		if (operands == null) {
			throw new IllegalArgumentException("Qualifier operands list cannot be null.");
		} else {
			if (operands.size() != operator.getNumOperands()) {
				throw new IllegalArgumentException(String.format(
						"Operator %s requires %s operands, not %s.", operator, operator.getNumOperands(), operands.size()));
			}
			for (Operand operand : operands) {
				if (operand == null) {
					throw new IllegalArgumentException("Qualifier operand cannot be null.");
				}
			}
		}
		this.operator = operator;
		this.operands = operands;
	}
	
	public Operator getOperator() {
		return this.operator;
	}
	
	public List<Operand> getOperands() {
		return this.operands;
	}
}
