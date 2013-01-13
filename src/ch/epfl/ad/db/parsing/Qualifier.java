package ch.epfl.ad.db.parsing;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * An SQL qualifier (a single qualifying expression that can appear in WHERE or HAVING clauses).
 * 
 * @author Artyom Stetsenko
 */
public class Qualifier {
	
	/**
	 * This qualifier's operator.
	 */
	private Operator operator;
	
	/**
	 * The operands of this qualifier's operator.
	 */
	private List<Operand> operands;
	
	/**
	 * Constructor of a qualifier.
	 * 
	 * @param operator
	 *                the qualifier's operator
	 * @param operand
	 *                the operand of the operator
	 */
	public Qualifier(Operator operator, Operand operand) {
		this(operator, new LinkedList<Operand>(Arrays.asList(operand)));
	}
	
	/**
	 * Constructor of a qualifier.
	 * 
	 * @param operator
	 *                the qualifier's operator
	 * @param operands
	 *                the operands of the operator
	 */
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
	
	/**
	 * Getter of this qualifier's operator.
	 * 
	 * @return this qualifier's operator
	 */
	public Operator getOperator() {
		return this.operator;
	}
	
	/**
	 * Getter of this qualifier's operator's operansds.
	 * 
	 * @return this qualifier's operator's operands
	 */
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
			string.append(operand.toString());
			if (operand instanceof QueryRelation && ((QueryRelation)operand).getAlias() == null) {
				string.append(")");
			}
			prefix = " AND ";
		}
		return string.toString();
	}
	
	/**
	 * Retrieves the final string representation of this qualifier. Final string representations
	 * are used when an aggregate query is run on the master node to merge intermediate
	 * aggregate results from the worker nodes (i.e. the second and final step of execution
	 * of this query).
	 * 
	 * @param intermediateRelation
	 *                the named relation on the master node holding intermediate results
	 *                from the worker nodes
	 * @param intermediateFields
	 *                the set of fields of intermediateRelation (used to identify the names
	 *                of this qualifier's field operands in intermediateRelation)
	 * @return
	 */
	public String toFinalString(NamedRelation intermediateRelation, List<Field> intermediateFields) {
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
			string.append(operand instanceof Field ? ((Field)operand).toFinalString(intermediateRelation, intermediateFields.indexOf((Field)operand)) : operand.toString());
			if (operand instanceof QueryRelation && ((QueryRelation)operand).getAlias() == null) {
				string.append(")");
			}
			prefix = " AND ";
		}
		return string.toString();
	}
}
