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
	
	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		if (this.operator.getNumOperands() > 1) {
			string.append(this.operands.get(0)).append(" ");
		}
		string.append(this.operator).append(" ");
		String prefix = "";
		for (int curOperand = this.operator.getNumOperands() > 1 ? 1 : 0; curOperand < this.operator.getNumOperands(); curOperand++) {
			string.append(prefix);
			Operand operand = this.operands.get(curOperand);
			if (operand instanceof QueryRelation && ((QueryRelation)operand).getAlias() == null) {
				string.append("(");
			}
			string.append(operand);
			if (operand instanceof QueryRelation && ((QueryRelation)operand).getAlias() == null) {
				string.append(")");
			}
			prefix = " AND ";
		}
		return string.toString();
	}
}
