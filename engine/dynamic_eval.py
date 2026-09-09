"""
railrisk/engine/dynamic_eval.py
-------------------------------
Safe dynamic mathematical expression evaluator for user-defined risk scoring rules.
Uses Python's native AST module to execute formulas without using `eval()` or `exec()`.
"""

import ast
import operator
from typing import Dict, Union


class SecurityError(ValueError):
    """Raised when an expression contains forbidden or unsafe AST constructs."""
    pass


class FormulaEvaluationError(ValueError):
    """Raised when formula parsing or execution fails (e.g. syntax, missing variables, division by zero)."""
    pass


# Allowed binary operator mappings
_ALLOWED_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
}

# Allowed unary operator mappings
_ALLOWED_UNARY_OPS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


def _eval_ast_node(node: ast.AST, context: Dict[str, float]) -> float:
    """
    Recursively evaluates an AST node strictly enforcing allowed mathematical constructs.
    """
    # Top-level expression container
    if isinstance(node, ast.Expression):
        return _eval_ast_node(node.body, context)

    # Numeric constants (integers and floats)
    elif isinstance(node, ast.Constant):
        # Exclude string, boolean, and non-numeric constants
        if isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
            return float(node.value)
        raise SecurityError(
            f"Forbidden constant type '{type(node.value).__name__}'. Only numeric literals are allowed."
        )

    # Named variables (e.g. delay_minutes, drift_rate_mph)
    elif isinstance(node, ast.Name):
        if not isinstance(node.ctx, ast.Load):
            raise SecurityError(f"Forbidden variable access context: {type(node.ctx).__name__}")
        if node.id not in context:
            raise FormulaEvaluationError(f"Undefined context variable: '{node.id}'")
        try:
            return float(context[node.id])
        except (ValueError, TypeError) as err:
            raise FormulaEvaluationError(
                f"Variable '{node.id}' value '{context[node.id]}' cannot be converted to float."
            ) from err

    # Binary operations (+, -, *, /, **)
    elif isinstance(node, ast.BinOp):
        op_type = type(node.op)
        if op_type not in _ALLOWED_BIN_OPS:
            raise SecurityError(f"Forbidden binary operator: '{op_type.__name__}'")

        left_val = _eval_ast_node(node.left, context)
        right_val = _eval_ast_node(node.right, context)

        if op_type is ast.Div and right_val == 0.0:
            raise FormulaEvaluationError("Division by zero in risk formula evaluation.")

        try:
            return float(_ALLOWED_BIN_OPS[op_type](left_val, right_val))
        except OverflowError as err:
            raise FormulaEvaluationError("Numeric overflow occurred during formula evaluation.") from err

    # Unary operations (+x, -x)
    elif isinstance(node, ast.UnaryOp):
        op_type = type(node.op)
        if op_type not in _ALLOWED_UNARY_OPS:
            raise SecurityError(f"Forbidden unary operator: '{op_type.__name__}'")

        operand_val = _eval_ast_node(node.operand, context)
        return float(_ALLOWED_UNARY_OPS[op_type](operand_val))

    # Reject all unsafe constructs (ast.Call, ast.Attribute, ast.Subscript, imports, etc.)
    else:
        raise SecurityError(f"Forbidden or unsafe expression construct: '{type(node).__name__}'")


def evaluate_formula(expression: str, context: Dict[str, float]) -> float:
    """
    Safely evaluates a user-defined mathematical risk formula against a variable context.

    Args:
        expression: Math string (e.g., "(100 - effective_buffer_mins) + (drift_rate_mph ** 2)")
        context: Map of variable names to numeric float values.

    Returns:
        float: Evaluated formula result.

    Raises:
        SecurityError: If expression contains function calls, attributes, or forbidden AST nodes.
        FormulaEvaluationError: If parsing fails, variables are missing, or division by zero occurs.
    """
    if not expression or not expression.strip():
        raise FormulaEvaluationError("Formula expression cannot be empty.")

    try:
        # Parse expression into an AST in single-expression mode
        parsed_ast = ast.parse(expression.strip(), mode="eval")
    except SyntaxError as err:
        raise FormulaEvaluationError(f"Invalid formula syntax: {err}") from err

    return _eval_ast_node(parsed_ast, context)