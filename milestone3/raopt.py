# -----------------------------------------------------------
# Milestone 2: Logical Optimization
# Author: Ecenaz Güngör – Matriculation number: 118802
# -----------------------------------------------------------

from radb import ast as ra
from radb.parse import RAParser as sym


def split_conjunction(pred):

    if isinstance(pred, ra.ValExprBinaryOp) and pred.op == sym.AND:
        left, right = pred.inputs
        return split_conjunction(left) + split_conjunction(right)
    return [pred]


def merge_conjunction(pred_list):

    if not pred_list:
        return None

    expr = pred_list[0]
    for p in pred_list[1:]:
        expr = ra.ValExprBinaryOp(expr, sym.AND, p)

    return expr


def collect_attribute_references(expr, out):

    if isinstance(expr, ra.AttrRef):
        out.add(expr)
    elif isinstance(expr, ra.ValExprBinaryOp):
        left, right = expr.inputs
        collect_attribute_references(left, out)
        collect_attribute_references(right, out)



def relation_aliases(node):

    names = set()

    if isinstance(node, ra.RelRef):
        names.add(node.rel)

    elif isinstance(node, ra.Rename):
        names.add(node.relname)
        names |= relation_aliases(node.inputs[0])

    elif isinstance(node, (ra.Project, ra.Select, ra.Aggr)):
        names |= relation_aliases(node.inputs[0])

    elif isinstance(node, (ra.Cross, ra.Join, ra.SetOp)):
        names |= relation_aliases(node.inputs[0])
        names |= relation_aliases(node.inputs[1])

    return names


def base_relation_names(node):

    names = set()

    if isinstance(node, ra.RelRef):
        names.add(node.rel)

    elif isinstance(node, ra.Rename):
        names |= base_relation_names(node.inputs[0])

    elif isinstance(node, (ra.Project, ra.Select, ra.Aggr)):
        names |= base_relation_names(node.inputs[0])

    elif isinstance(node, (ra.Cross, ra.Join, ra.SetOp)):
        names |= base_relation_names(node.inputs[0])
        names |= base_relation_names(node.inputs[1])

    return names


def build_attribute_to_relation_map(schema_dict):
    mapping = {}
    for rel, attrs in schema_dict.items():
        for attr in attrs:
            mapping.setdefault(attr, set()).add(rel)
    return mapping



def rule_break_up_selections(expr):
    if isinstance(expr, ra.Select):
        child = rule_break_up_selections(expr.inputs[0])
        preds = split_conjunction(expr.cond)

        # Add them one by one, inside out
        out = child
        for p in reversed(preds):
            out = ra.Select(p, out)
        return out

    # Otherwise just recurse
    if isinstance(expr, ra.Project):
        return ra.Project(expr.attrs, rule_break_up_selections(expr.inputs[0]))

    if isinstance(expr, ra.Rename):
        return ra.Rename(expr.relname, expr.attrnames,
                         rule_break_up_selections(expr.inputs[0]))

    if isinstance(expr, ra.Cross):
        return ra.Cross(
            rule_break_up_selections(expr.inputs[0]),
            rule_break_up_selections(expr.inputs[1])
        )

    if isinstance(expr, ra.Join):
        return ra.Join(
            rule_break_up_selections(expr.inputs[0]),
            expr.cond,
            rule_break_up_selections(expr.inputs[1])
        )

    if isinstance(expr, ra.SetOp):
        return type(expr)(
            rule_break_up_selections(expr.inputs[0]),
            rule_break_up_selections(expr.inputs[1])
        )

    return expr



def _analyze_predicate_sides(cond, left_expr, right_expr, schema_dict, attr_to_rel):
    """
    Figure out whether a condition uses attributes from
    the left child, right child, or both.
    """
    attrs = set()
    collect_attribute_references(cond, attrs)

    left_alias = relation_aliases(left_expr)
    right_alias = relation_aliases(right_expr)

    left_base = base_relation_names(left_expr)
    right_base = base_relation_names(right_expr)

    left_used = False
    right_used = False

    for a in attrs:
        if a.rel is not None:
            if a.rel in left_alias:
                left_used = True
            if a.rel in right_alias:
                right_used = True
        else:
            candidates = attr_to_rel.get(a.name, set())
            if any(r in left_base for r in candidates):
                left_used = True
            if any(r in right_base for r in candidates):
                right_used = True

    return left_used, right_used



def _push_selection(cond, expr, schema_dict, attr_to_rel):

    if isinstance(expr, ra.Select):
        inner = expr.inputs[0]

        # Special case
        if isinstance(inner, ra.Cross):
            left, right = inner.inputs

            uses_left_outer, uses_right_outer = _analyze_predicate_sides(cond, left, right, schema_dict, attr_to_rel)
            uses_left_inner, uses_right_inner = _analyze_predicate_sides(expr.cond, left, right, schema_dict, attr_to_rel)

            # inner cond is join outer cond touches exactly one side
            if (uses_left_inner and uses_right_inner) and (uses_left_outer != uses_right_outer):
                if uses_left_outer:
                    new_left = _push_selection(cond, left, schema_dict, attr_to_rel)
                    new_cross = ra.Cross(new_left, right)
                else:
                    new_right = _push_selection(cond, right, schema_dict, attr_to_rel)
                    new_cross = ra.Cross(left, new_right)

                return ra.Select(expr.cond, new_cross)

        # default
        return ra.Select(cond, expr)

    # Dont push through rename
    if isinstance(expr, ra.Rename):
        return ra.Select(cond, expr)

    # CROSS case
    if isinstance(expr, ra.Cross):
        left, right = expr.inputs
        use_left, use_right = _analyze_predicate_sides(cond, left, right, schema_dict, attr_to_rel)

        if use_left and not use_right:
            return ra.Cross(_push_selection(cond, left, schema_dict, attr_to_rel), right)
        if use_right and not use_left:
            return ra.Cross(left, _push_selection(cond, right, schema_dict, attr_to_rel))

        return ra.Select(cond, expr)

    return ra.Select(cond, expr)



def rule_push_down_selections(expr, schema_dict):
    """
    Apply selection pushdown recursively.
    """
    attr_to_rel = build_attribute_to_relation_map(schema_dict)

    def recurse(n):
        if isinstance(n, ra.Select):
            child = recurse(n.inputs[0])
            return _push_selection(n.cond, child, schema_dict, attr_to_rel)

        if isinstance(n, ra.Project):
            return ra.Project(n.attrs, recurse(n.inputs[0]))

        if isinstance(n, ra.Rename):
            return ra.Rename(n.relname, n.attrnames, recurse(n.inputs[0]))

        if isinstance(n, ra.Cross):
            return ra.Cross(recurse(n.inputs[0]), recurse(n.inputs[1]))

        if isinstance(n, ra.Join):
            return ra.Join(recurse(n.inputs[0]), n.cond, recurse(n.inputs[1]))

        if isinstance(n, ra.SetOp):
            return type(n)(recurse(n.inputs[0]), recurse(n.inputs[1]))

        return n

    return recurse(expr)



def rule_merge_selections(expr):
    """
    Just put the predicates into one AND-chain.
    """
    if isinstance(expr, ra.Select):
        child = rule_merge_selections(expr.inputs[0])

        if isinstance(child, ra.Select):
            all_preds = split_conjunction(expr.cond) + split_conjunction(child.cond)
            merged = merge_conjunction(all_preds)
            return ra.Select(merged, child.inputs[0])

        return ra.Select(expr.cond, child)

    # Otherwise just recurse normally
    if isinstance(expr, ra.Project):
        return ra.Project(expr.attrs, rule_merge_selections(expr.inputs[0]))

    if isinstance(expr, ra.Rename):
        return ra.Rename(expr.relname, expr.attrnames,
                         rule_merge_selections(expr.inputs[0]))

    if isinstance(expr, ra.Cross):
        return ra.Cross(rule_merge_selections(expr.inputs[0]),
                        rule_merge_selections(expr.inputs[1]))

    if isinstance(expr, ra.Join):
        return ra.Join(rule_merge_selections(expr.inputs[0]),
                       expr.cond,
                       rule_merge_selections(expr.inputs[1]))

    if isinstance(expr, ra.SetOp):
        return type(expr)(
            rule_merge_selections(expr.inputs[0]),
            rule_merge_selections(expr.inputs[1])
        )

    return expr



def _is_join_condition(cond):
    """
    A join condition references attributes from at least 2 relations.
    """
    attrs = set()
    collect_attribute_references(cond, attrs)
    rels = {a.rel for a in attrs if a.rel is not None}
    return len(rels) >= 2


def rule_introduce_joins(expr):

    if isinstance(expr, ra.Select):
        child = rule_introduce_joins(expr.inputs[0])

        if isinstance(child, ra.Cross) and _is_join_condition(expr.cond):
            left = rule_introduce_joins(child.inputs[0])
            right = rule_introduce_joins(child.inputs[1])
            return ra.Join(left, expr.cond, right)

        return ra.Select(expr.cond, child)

    if isinstance(expr, ra.Project):
        return ra.Project(expr.attrs, rule_introduce_joins(expr.inputs[0]))

    if isinstance(expr, ra.Rename):
        return ra.Rename(expr.relname, expr.attrnames,
                         rule_introduce_joins(expr.inputs[0]))

    if isinstance(expr, ra.Cross):
        return ra.Cross(
            rule_introduce_joins(expr.inputs[0]),
            rule_introduce_joins(expr.inputs[1])
        )

    if isinstance(expr, ra.Join):
        return ra.Join(
            rule_introduce_joins(expr.inputs[0]),
            expr.cond,
            rule_introduce_joins(expr.inputs[1])
        )

    if isinstance(expr, ra.SetOp):
        return type(expr)(
            rule_introduce_joins(expr.inputs[0]),
            rule_introduce_joins(expr.inputs[1])
        )

    return expr
