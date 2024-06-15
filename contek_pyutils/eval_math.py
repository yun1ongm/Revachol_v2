import ast
import operator as op

# supported operators
operators = {
    ast.Add: op.add,
    ast.Sub: op.sub,
    ast.Mult: op.mul,
    ast.Div: op.truediv,
    ast.Pow: op.pow,
    ast.BitXor: op.xor,
    ast.USub: op.neg,
}


def eval_yaml_node(node):
    if isinstance(node, list):
        return [eval_yaml_node(x) for x in node]
    elif isinstance(node, dict):
        return {k: eval_yaml_node(v) for (k, v) in node.items()}
    elif node == "None":
        return None
    else:
        # noinspection PyBroadException
        try:
            return eval_expr(node)
        except Exception:
            return node


def eval_expr(expr):
    """
    >>> eval_expr('2^6')
    4
    >>> eval_expr('2**6')
    64
    >>> eval_expr('1 + 2*3**(4^5) / (6 + -7)')
    -5.0
    """
    return eval_(ast.parse(expr, mode="eval").body)


def eval_(node):
    if isinstance(node, ast.Num):  # <number>
        return node.n
    elif isinstance(node, ast.BinOp):  # <left> <operator> <right>
        # noinspection PyTypeChecker
        return operators[type(node.op)](eval_(node.left), eval_(node.right))  # type: ignore
    elif isinstance(node, ast.UnaryOp):  # <operator> <operand> e.g., -1
        # noinspection PyTypeChecker
        return operators[type(node.op)](eval_(node.operand))  # type: ignore
    else:
        raise TypeError(node)


if __name__ == "__main__":
    print(eval_expr("2**6"))
