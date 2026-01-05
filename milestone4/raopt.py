# -----------------------------------------------------------
# Milestone 2 (+ Milestone 4 extension): Logical Optimization
# Author: Ecenaz Güngör – Matriculation number: 118802
# -----------------------------------------------------------

from radb import ast as ra
from radb.parse import RAParser as sym


# =========================
# Helpers for selections
# =========================

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


# =========================
# Helpers for alias / base names
# =========================

def relation_aliases(node):
    names = set()

    if isinstance(node, ra.RelRef):
        names.add(node.rel)

    elif isinstance(node, ra.Rename):
        # rename introduces new alias
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
    """
    schema_dict maps relation -> { attrName -> typeStr }  (as in miniHive.py dd)
    returns attrName -> set(relations containing that attribute)
    """
    mapping = {}
    for rel, attrs in schema_dict.items():
        for attr in attrs:
            mapping.setdefault(attr, set()).add(rel)
    return mapping


# =========================
# Rule 1: break up selections
# =========================

def rule_break_up_selections(expr):
    if isinstance(expr, ra.Select):
        child = rule_break_up_selections(expr.inputs[0])
        preds = split_conjunction(expr.cond)

        out = child
        for p in reversed(preds):
            out = ra.Select(p, out)
        return out

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


# =========================
# Selection pushdown internals
# =========================

def _analyze_predicate_sides(cond, left_expr, right_expr, schema_dict, attr_to_rel):
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

        if isinstance(inner, ra.Cross):
            left, right = inner.inputs

            uses_left_outer, uses_right_outer = _analyze_predicate_sides(
                cond, left, right, schema_dict, attr_to_rel
            )
            uses_left_inner, uses_right_inner = _analyze_predicate_sides(
                expr.cond, left, right, schema_dict, attr_to_rel
            )

            if (uses_left_inner and uses_right_inner) and (uses_left_outer != uses_right_outer):
                if uses_left_outer:
                    new_left = _push_selection(cond, left, schema_dict, attr_to_rel)
                    new_cross = ra.Cross(new_left, right)
                else:
                    new_right = _push_selection(cond, right, schema_dict, attr_to_rel)
                    new_cross = ra.Cross(left, new_right)

                return ra.Select(expr.cond, new_cross)

        return ra.Select(cond, expr)

    if isinstance(expr, ra.Rename):
        return ra.Select(cond, expr)

    if isinstance(expr, ra.Cross):
        left, right = expr.inputs
        use_left, use_right = _analyze_predicate_sides(cond, left, right, schema_dict, attr_to_rel)

        if use_left and not use_right:
            return ra.Cross(_push_selection(cond, left, schema_dict, attr_to_rel), right)
        if use_right and not use_left:
            return ra.Cross(left, _push_selection(cond, right, schema_dict, attr_to_rel))

        return ra.Select(cond, expr)

    return ra.Select(cond, expr)


# =========================
# Rule 2: push down selections
# =========================

def rule_push_down_selections(expr, schema_dict):
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


# =========================
# Rule 3: merge selections
# =========================

def rule_merge_selections(expr):
    if isinstance(expr, ra.Select):
        child = rule_merge_selections(expr.inputs[0])

        if isinstance(child, ra.Select):
            all_preds = split_conjunction(expr.cond) + split_conjunction(child.cond)
            merged = merge_conjunction(all_preds)
            return ra.Select(merged, child.inputs[0])

        return ra.Select(expr.cond, child)

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


# =========================
# Rule 4: introduce joins
# =========================

def _is_join_condition(cond):
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


# ===========================================================
# Milestone 4: Projection pushdown (SAFE, join-width reduction)
# ===========================================================

def _attr_key(a: ra.AttrRef):
    return (a.rel, a.name)


def _make_attrref(rel, name):
    return ra.AttrRef(rel, name)


def _collect_attr_keys_from_expr(expr):
    attrs = set()
    collect_attribute_references(expr, attrs)
    return {_attr_key(a) for a in attrs}


def _collect_attr_keys_from_attrlist(attr_list):
    return {_attr_key(a) for a in attr_list}


def _required_names(required_keys):
    """Drop rel qualifier: keep only attribute names."""
    return {name for (_rel, name) in required_keys}


def _split_required_for_children(required_keys, left_expr, right_expr, schema_dict, attr_to_rel):
    """
    Conservative splitting:
      - Qualified attrs go to the side whose aliases contain that qualifier.
      - Unqualified attrs:
           if unique to one side -> only that side
           else (ambiguous) -> BOTH sides (safe)
    """
    left_alias = relation_aliases(left_expr)
    right_alias = relation_aliases(right_expr)

    left_base = base_relation_names(left_expr)
    right_base = base_relation_names(right_expr)

    left_req = set()
    right_req = set()

    for (rel, name) in required_keys:
        if rel is not None:
            if rel in left_alias:
                left_req.add((None, name))   # project below as unqualified (works with your MR keys)
            if rel in right_alias:
                right_req.add((None, name))
        else:
            candidates = attr_to_rel.get(name, set())
            in_left = any(r in left_base for r in candidates)
            in_right = any(r in right_base for r in candidates)

            # ambiguous -> keep on both sides (safe)
            if in_left:
                left_req.add((None, name))
            if in_right:
                right_req.add((None, name))

            # if schema doesn't know, keep on both sides to avoid dropping
            if not candidates:
                left_req.add((None, name))
                right_req.add((None, name))

    return left_req, right_req


def _all_attrs_for_relation(relname, schema_dict):
    if relname not in schema_dict:
        return set()
    return {(None, a) for a in schema_dict[relname].keys()}


def rule_push_down_projections(expr, schema_dict):
    """
    SAFE projection pushdown:
      - reduces width before joins/crosses (major cost savings)
      - pushes through Rename as "relation rename only" (matches your MR RenameTask behavior)
      - never risks dropping join/selection predicate attributes
      - conservative for ambiguous unqualified attrs
    """
    attr_to_rel = build_attribute_to_relation_map(schema_dict)

    def recurse(node, required_keys):

        if isinstance(node, ra.Join) and required_keys:
            left, right = node.inputs

            left_aliases = relation_aliases(left)
            right_aliases = relation_aliases(right)

            used_left = any(rel in left_aliases for (rel, _) in required_keys if rel is not None)
            used_right = any(rel in right_aliases for (rel, _) in required_keys if rel is not None)
 

            if used_left ^ used_right:
                return node
            
        if isinstance(node, ra.RelRef):
            if not required_keys:
                return node
            all_keys = _all_attrs_for_relation(node.rel, schema_dict)
            keep = required_keys & all_keys
            if not keep:
                return node
            attrs = [_make_attrref(None, name) for (_r, name) in sorted(keep)]
            return ra.Project(attrs, node)

        # Rename: push required names below (unqualified),
        # because your runtime keys use suffix matching and RenameTask rewrites prefixes.
        if isinstance(node, ra.Rename):
            # below rename, request by name only
            child_required = {(None, n) for n in _required_names(required_keys)} if required_keys else set()
            new_child = recurse(node.inputs[0], child_required)
            return ra.Rename(node.relname, node.attrnames, new_child)

        # Project: its own attr list defines what is available above
        if isinstance(node, ra.Project):
            proj_keys = _collect_attr_keys_from_attrlist(node.attrs)

            if required_keys:
                child_required = proj_keys & required_keys
                if not child_required:
                    child_required = proj_keys
            else:
                child_required = proj_keys

            new_child = recurse(node.inputs[0], child_required)
            return ra.Project(node.attrs, new_child)

        # Select: must keep attrs needed by predicate
        if isinstance(node, ra.Select):
            cond_keys = _collect_attr_keys_from_expr(node.cond)
            child_required = set(required_keys) | cond_keys
            new_child = recurse(node.inputs[0], child_required)
            return ra.Select(node.cond, new_child)

        # Join: must keep attrs needed by join cond + above requirements
        if isinstance(node, ra.Join):
            cond_keys = _collect_attr_keys_from_expr(node.cond)
            needed = set(required_keys) | cond_keys

            left, right = node.inputs
            left_req, right_req = _split_required_for_children(needed, left, right, schema_dict, attr_to_rel)

            new_left = recurse(left, left_req)
            new_right = recurse(right, right_req)
            return ra.Join(new_left, node.cond, new_right)

        # Cross: split requirements conservatively
        if isinstance(node, ra.Cross):
            left, right = node.inputs
            left_req, right_req = _split_required_for_children(set(required_keys), left, right, schema_dict, attr_to_rel)
            new_left = recurse(left, left_req)
            new_right = recurse(right, right_req)
            return ra.Cross(new_left, new_right)

        # Set operations: require same schema on both sides
        if isinstance(node, ra.SetOp):
            left = recurse(node.inputs[0], set(required_keys))
            right = recurse(node.inputs[1], set(required_keys))
            return type(node)(left, right)

        return node

    return recurse(expr, set())


# ===========================================================
# Milestone 4: SAFE micro-optimization
# remove redundant nested projects
# ===========================================================

def rule_remove_redundant_projects(expr):
    if isinstance(expr, ra.Project):
        child = rule_remove_redundant_projects(expr.inputs[0])

        if isinstance(child, ra.Project):
            if expr.attrs == child.attrs:
                return ra.Project(expr.attrs, child.inputs[0])

        return ra.Project(expr.attrs, child)

    if isinstance(expr, ra.Select):
        return ra.Select(expr.cond, rule_remove_redundant_projects(expr.inputs[0]))

    if isinstance(expr, ra.Rename):
        return ra.Rename(expr.relname, expr.attrnames,
                         rule_remove_redundant_projects(expr.inputs[0]))

    if isinstance(expr, ra.Join):
        return ra.Join(
            rule_remove_redundant_projects(expr.inputs[0]),
            expr.cond,
            rule_remove_redundant_projects(expr.inputs[1])
        )

    if isinstance(expr, ra.Cross):
        return ra.Cross(
            rule_remove_redundant_projects(expr.inputs[0]),
            rule_remove_redundant_projects(expr.inputs[1])
        )

    return expr

def rule_remove_redundant_selects(expr):
    if isinstance(expr, ra.Select):
        child = rule_remove_redundant_selects(expr.inputs[0])
        if isinstance(child, ra.Select):
            return ra.Select(
                ra.ValExprBinaryOp(expr.cond, sym.AND, child.cond),
                child.inputs[0]
            )
        return ra.Select(expr.cond, child)

    if hasattr(expr, "inputs"):
        expr.inputs = [rule_remove_redundant_selects(c) for c in expr.inputs]
    return expr

def rule_reorder_joins(expr):
    if isinstance(expr, ra.Join):
        left = rule_reorder_joins(expr.inputs[0])
        right = rule_reorder_joins(expr.inputs[1])

        # Heuristic: prefer Select(...) on the left
        if isinstance(right, ra.Select) and not isinstance(left, ra.Select):
            return ra.Join(right, expr.cond, left)

        return ra.Join(left, expr.cond, right)

    if hasattr(expr, "inputs"):
        expr.inputs = [rule_reorder_joins(c) for c in expr.inputs]
    return expr


def rule_remove_redundant_distinct(expr, schema_dict):
    if isinstance(expr, ra.Project):
        if len(expr.attrs) == 1:
            attr = expr.attrs[0]
            if attr.name.endswith("KEY"):
                return rule_remove_redundant_distinct(expr.inputs[0], schema_dict)
    return expr

def rule_reorder_joins(expr):
    if isinstance(expr, ra.Join):
        left = rule_reorder_joins(expr.inputs[0])
        right = rule_reorder_joins(expr.inputs[1])

        # Heuristic: prefer Select(...) on the left
        if isinstance(right, ra.Select) and not isinstance(left, ra.Select):
            return ra.Join(right, expr.cond, left)

        return ra.Join(left, expr.cond, right)

    if hasattr(expr, "inputs"):
        expr.inputs = [rule_reorder_joins(c) for c in expr.inputs]
    return expr
