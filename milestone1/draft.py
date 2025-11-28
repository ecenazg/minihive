import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML, Wildcard, Number, String, Operator

from radb import ast as ra_ast

# get access to operator constants (EQ, AND, etc.)
sym = ra_ast.sym



# ---------------------------------------------------------------------------
# Helper: Convert Identifier -> AttrRef
# ---------------------------------------------------------------------------
def _attr_from_identifier(ident: Identifier) -> ra_ast.AttrRef:
    """
    Convert a SQL identifier like 'Person.name' or 'name'
    into a radb AttrRef('Person', 'name') or AttrRef(None, 'name').
    """
    rel = ident.get_parent_name()   # table name if present
    name = ident.get_real_name()    # attribute name
    return ra_ast.AttrRef(rel, name)


# ---------------------------------------------------------------------------
# Helper: Extract SELECT list
# ---------------------------------------------------------------------------
def _extract_select_list(stmt):
    """
    Returns (is_star, attrs)
    - is_star: True if SELECT DISTINCT * ...
    - attrs: list of ra_ast.AttrRef if not star
    """
    select_seen = False
    attrs = []
    is_star = False

    for token in stmt.tokens:
        if token.ttype is DML and token.value.upper() == 'SELECT':
            select_seen = True
            continue

        if not select_seen:
            continue

        # Skip DISTINCT keyword
        if token.ttype is Keyword and token.value.upper() == 'DISTINCT':
            continue

        # Stop when FROM begins
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            break

        if token.is_whitespace:
            continue

        # SELECT list with commas
        if isinstance(token, IdentifierList):
            for ident in token.get_identifiers():
                attrs.append(_attr_from_identifier(ident))
            continue

        # Single identifier
        if isinstance(token, Identifier):
            attrs.append(_attr_from_identifier(token))
            continue

        # Wildcard: *
        if token.ttype is Wildcard:
            is_star = True
            break

    return is_star, attrs


# ---------------------------------------------------------------------------
# Helper: Extract FROM items
# ---------------------------------------------------------------------------
def _extract_from_items(stmt):
    """
    Return a list of (table_name, alias_or_None) from the FROM clause.
    """
    from_seen = False
    items = []

    for token in stmt.tokens:
        if token.is_whitespace:
            continue

        if token.ttype is DML and token.value.upper() == 'SELECT':
            continue

        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True
            continue

        if not from_seen:
            continue

        # Stop when WHERE starts
        if isinstance(token, Where):
            break

        # Multiple identifiers: e.g. Person, Eats
        if isinstance(token, IdentifierList):
            for ident in token.get_identifiers():
                table = ident.get_real_name()
                alias = ident.get_alias()
                items.append((table, alias))
            continue

        # Single identifier: e.g. Course C
        if isinstance(token, Identifier):
            table = token.get_real_name()
            alias = token.get_alias()
            items.append((table, alias))
            continue

    return items


# ---------------------------------------------------------------------------
# Helper: Build RA expression for FROM clause
# ---------------------------------------------------------------------------
def _build_from_expr(from_items):
    """
    Convert from_items [(table, alias_or_None), ...] into a RA expression
    using RelRef, Rename, and Cross.
    """
    rel_exprs = []

    for table, alias in from_items:
        base = ra_ast.RelRef(table)
        if alias is not None:
            base = ra_ast.Rename(alias, None, base)
        rel_exprs.append(base)

    if not rel_exprs:
        return None

    expr = rel_exprs[0]
    for other in rel_exprs[1:]:
        expr = ra_ast.Cross(expr, other)

    return expr


# ---------------------------------------------------------------------------
# Helper: Convert token -> ValExpr (AttrRef, RANumber, RAString)
# ---------------------------------------------------------------------------
def _valexpr_from_token(token):
    if isinstance(token, Identifier):
        return _attr_from_identifier(token)

    if token.ttype in Number:
        return ra_ast.RANumber(token.value)

    if token.ttype in String:
        return ra_ast.RAString(token.value)

    # Default fallback: treat as string literal (quoted constant)
    return ra_ast.RAString(token.value)


# ---------------------------------------------------------------------------
# Helper: Convert Comparison -> ValExprBinaryOp
# ---------------------------------------------------------------------------
def _comparison_to_valexpr(comp: Comparison):
    """Convert a simple equality comparison into a ValExprBinaryOp using radb symbols."""
    left_tok = None
    right_tok = None
    seen_op = False

    for tok in comp.tokens:
        if tok.is_whitespace:
            continue
        if tok.ttype is Operator.Comparison or tok.value == "=":
            seen_op = True
            continue
        if not seen_op and left_tok is None:
            left_tok = tok
        elif seen_op and right_tok is None:
            right_tok = tok

    left = _valexpr_from_token(left_tok)
    right = _valexpr_from_token(right_tok)

    # use proper radb symbol constants
    return ra_ast.ValExprBinaryOp(left, sym.EQ, right)

# ---------------------------------------------------------------------------
# Helper: Extract WHERE condition as ValExpr
# ---------------------------------------------------------------------------
def _extract_where_condition(stmt):
    """Extract WHERE clause as nested AND of ValExprBinaryOps."""
    where_tok = None
    for token in stmt.tokens:
        if isinstance(token, Where):
            where_tok = token
            break

    if where_tok is None:
        return None

    comparisons = [t for t in where_tok.tokens if isinstance(t, Comparison)]
    if not comparisons:
        return None

    cond = _comparison_to_valexpr(comparisons[0])
    for more in comparisons[1:]:
        cond = ra_ast.ValExprBinaryOp(cond, sym.AND, _comparison_to_valexpr(more))

    return cond


# ---------------------------------------------------------------------------
# Main translation function
# ---------------------------------------------------------------------------
def translate(stmt):
    """
    Translate a parsed SQL statement into a radb AST expression.
    """
    # 1. FROM
    from_items = _extract_from_items(stmt)
    ra = _build_from_expr(from_items)

    # 2. WHERE
    cond = _extract_where_condition(stmt)
    if cond is not None:
        ra = ra_ast.Select(cond, ra)

    # 3. SELECT
    is_star, attrs = _extract_select_list(stmt)
    if not is_star:
        ra = ra_ast.Project(attrs, ra)

    return ra
