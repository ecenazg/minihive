# Ecenaz Güngör - Matriculation number: 118802

from radb import ast as ra
from radb.parse import RAParser as sym


def split_conj(pred):

    if isinstance(pred, ra.ValExprBinaryOp) and pred.op == sym.AND:
        left, right = pred.inputs
        return split_conj(left) + split_conj(right)
    else:
        return [pred]


def merge_conj(preds):

    if not preds:
        return None
    expr = preds[0]
    for p in preds[1:]:
        expr = ra.ValExprBinaryOp(expr, sym.AND, p)
    return expr


def collect_attrrefs(expr, out):

    if isinstance(expr, ra.AttrRef):
        out.add(expr)
    elif isinstance(expr, ra.ValExprBinaryOp):
        left, right = expr.inputs
        collect_attrrefs(left, out)
        collect_attrrefs(right, out)


def relnames_in_expr(relexpr):

    names = set()
    if isinstance(relexpr, ra.RelRef):
        names.add(relexpr.rel)
    elif isinstance(relexpr, ra.Rename):
        names.add(relexpr.relname)
        names |= relnames_in_expr(relexpr.inputs[0])
    elif isinstance(relexpr, (ra.Project, ra.Select, ra.Aggr)):
        names |= relnames_in_expr(relexpr.inputs[0])
    elif isinstance(relexpr, (ra.Cross, ra.Join, ra.SetOp)):
        names |= relnames_in_expr(relexpr.inputs[0])
        names |= relnames_in_expr(relexpr.inputs[1])
    return names


def base_relnames_in_expr(relexpr):

    names = set()
    if isinstance(relexpr, ra.RelRef):
        names.add(relexpr.rel)
    elif isinstance(relexpr, ra.Rename):
        names |= base_relnames_in_expr(relexpr.inputs[0])
    elif isinstance(relexpr, (ra.Project, ra.Select, ra.Aggr)):
        names |= base_relnames_in_expr(relexpr.inputs[0])
    elif isinstance(relexpr, (ra.Cross, ra.Join, ra.SetOp)):
        names |= base_relnames_in_expr(relexpr.inputs[0])
        names |= base_relnames_in_expr(relexpr.inputs[1])
    return names


def build_attr_to_rel_map(dd):

    attr_to_rel = {}
    for rel, attrs in dd.items():
        for a in attrs:
            attr_to_rel.setdefault(a, set()).add(rel)
    return attr_to_rel



def rule_break_up_selections(node):

    if isinstance(node, ra.Select):
        # Önce alt ağacı optimize et
        child = rule_break_up_selections(node.inputs[0])

        # Koşulu parçalara ayır
        preds = split_conj(node.cond)
        # p1 AND p2 AND p3  ->  σ_p1(σ_p2(σ_p3(R)))
        result = child
        # içte p3, onun üstüne p2, en dışa p1
        for p in reversed(preds):
            result = ra.Select(p, result)
        return result

    # Diğer RA düğümleri için sadece rekürsif olarak ilerle
    if isinstance(node, ra.Project):
        return ra.Project(node.attrs, rule_break_up_selections(node.inputs[0]))
    if isinstance(node, ra.Rename):
        return ra.Rename(node.relname, node.attrnames,
                         rule_break_up_selections(node.inputs[0]))
    if isinstance(node, ra.Cross):
        return ra.Cross(rule_break_up_selections(node.inputs[0]),
                        rule_break_up_selections(node.inputs[1]))
    if isinstance(node, ra.Join):
        return ra.Join(rule_break_up_selections(node.inputs[0]),
                       node.cond,
                       rule_break_up_selections(node.inputs[1]))
    if isinstance(node, ra.SetOp):
        return type(node)(rule_break_up_selections(node.inputs[0]),
                          rule_break_up_selections(node.inputs[1]))
    # RelRef vs. diğer yapraklar
    return node




def _analyze_cond_sides(cond, left, right, dd, attr_to_rel):

    attrs = set()
    collect_attrrefs(cond, attrs)

    left_aliases = relnames_in_expr(left)
    right_aliases = relnames_in_expr(right)
    left_bases = base_relnames_in_expr(left)
    right_bases = base_relnames_in_expr(right)

    uses_left = False
    uses_right = False

    for a in attrs:
        if a.rel is not None:
            # Qualifed attribute: Person.name, Eats.pizza, Eats1.pizza, ...
            if a.rel in left_aliases:
                uses_left = True
            if a.rel in right_aliases:
                uses_right = True
        else:
            # Unqualified attribute: gender, age, pizza, ...
            rel_candidates = attr_to_rel.get(a.name, set())
            if any(r in left_bases for r in rel_candidates):
                uses_left = True
            if any(r in right_bases for r in rel_candidates):
                uses_right = True

    return uses_left, uses_right


def _push_select_into(cond, expr, dd, attr_to_rel):

    # 1) Eğer expr zaten bir SELECT ise:
    if isinstance(expr, ra.Select):
        inner = expr.inputs[0]

        # 1.a) Özel durum: içteki seçim bir CROSS üzerinde ve join koşulu
        #      (her iki tarafa da dokunuyor),
        #      dıştaki seçim ise SADECE tek tarafa dokunuyorsa:
        #
        #      σ_outer( σ_join( left × right ) )
        #      ==>  σ_join( (σ_outer left) × right )   (veya sağ tarafa)
        if isinstance(inner, ra.Cross):
            left, right = inner.inputs

            uses_left_outer, uses_right_outer = _analyze_cond_sides(cond, left, right, dd, attr_to_rel)
            uses_left_inner, uses_right_inner = _analyze_cond_sides(expr.cond, left, right, dd, attr_to_rel)

            # iç seçim (expr.cond) join koşulu: her iki tarafı da kullanıyor
            # dış seçim (cond) yalnızca TEK tarafı kullanıyor
            outer_one_side = (uses_left_outer and not uses_right_outer) or (uses_right_outer and not uses_left_outer)

            if uses_left_inner and uses_right_inner and outer_one_side:
                # Dış koşulu uygun tarafa it
                if uses_left_outer and not uses_right_outer:
                    new_left = _push_select_into(cond, left, dd, attr_to_rel)
                    new_cross = ra.Cross(new_left, right)
                else:
                    new_right = _push_select_into(cond, right, dd, attr_to_rel)
                    new_cross = ra.Cross(left, new_right)

                # Join koşulu (inner cond) en üstte kalır
                return ra.Select(expr.cond, new_cross)

        # 1.b) Diğer tüm SELECT üzeri durumlarda, sıralamayı değiştirmeden
        #      dış koşulu ÜSTE koyuyoruz:
        #
        #      σ_outer( σ_inner(R) )
        return ra.Select(cond, expr)

    # 2) RENAME üzerinden seçim itme: testler bunu istemiyor, üstte kalacak.
    if isinstance(expr, ra.Rename):
        return ra.Select(cond, expr)

    # 3) CROSS üzerinde seçim: uygun tarafta bulunan relation’a doğru it
    if isinstance(expr, ra.Cross):
        left, right = expr.inputs
        uses_left, uses_right = _analyze_cond_sides(cond, left, right, dd, attr_to_rel)

        if uses_left and not uses_right:
            # Koşul sadece sol tarafa ait -> sola it
            new_left = _push_select_into(cond, left, dd, attr_to_rel)
            return ra.Cross(new_left, right)
        elif uses_right and not uses_left:
            # Koşul sadece sağ tarafa ait -> sağa it
            new_right = _push_select_into(cond, right, dd, attr_to_rel)
            return ra.Cross(left, new_right)
        else:
            # Her iki tarafı da kullanıyorsa veya hiçbirini kullanmıyorsa,
            # daha fazla itmeyip burada bırak.
            return ra.Select(cond, expr)

    # 4) Diğer operatörlere (Project, Join, Aggr, SetOp vs.) altına itmeyelim;
    #    burada seçimi durdur.
    return ra.Select(cond, expr)



def rule_push_down_selections(node, dd):

    attr_to_rel = build_attr_to_rel_map(dd)

    def helper(n):
        if isinstance(n, ra.Select):
            # Önce alt ağaçtaki diğer seçimler itilsin
            child = helper(n.inputs[0])
            # Sonra bu seçimi child içine it
            return _push_select_into(n.cond, child, dd, attr_to_rel)

        if isinstance(n, ra.Project):
            return ra.Project(n.attrs, helper(n.inputs[0]))
        if isinstance(n, ra.Rename):
            return ra.Rename(n.relname, n.attrnames, helper(n.inputs[0]))
        if isinstance(n, ra.Cross):
            return ra.Cross(helper(n.inputs[0]), helper(n.inputs[1]))
        if isinstance(n, ra.Join):
            return ra.Join(helper(n.inputs[0]), n.cond, helper(n.inputs[1]))
        if isinstance(n, ra.SetOp):
            return type(n)(helper(n.inputs[0]), helper(n.inputs[1]))
        return n

    return helper(node)



def rule_merge_selections(node):

    if isinstance(node, ra.Select):
        child = rule_merge_selections(node.inputs[0])

        # Eğer alt tarafta yine bir seçim varsa, birleşebilir
        if isinstance(child, ra.Select):
            # Dış koşul ÖNCE, sonra iç koşul -> orijinal sırayı koru
            preds = split_conj(node.cond) + split_conj(child.cond)
            merged = merge_conj(preds)
            return ra.Select(merged, child.inputs[0])
        else:
            return ra.Select(node.cond, child)

    if isinstance(node, ra.Project):
        return ra.Project(node.attrs, rule_merge_selections(node.inputs[0]))
    if isinstance(node, ra.Rename):
        return ra.Rename(node.relname, node.attrnames,
                         rule_merge_selections(node.inputs[0]))
    if isinstance(node, ra.Cross):
        return ra.Cross(rule_merge_selections(node.inputs[0]),
                        rule_merge_selections(node.inputs[1]))
    if isinstance(node, ra.Join):
        return ra.Join(rule_merge_selections(node.inputs[0]),
                       node.cond,
                       rule_merge_selections(node.inputs[1]))
    if isinstance(node, ra.SetOp):
        return type(node)(rule_merge_selections(node.inputs[0]),
                          rule_merge_selections(node.inputs[1]))
    return node



def _is_join_condition(cond):

    attrs = set()
    collect_attrrefs(cond, attrs)
    rels = {a.rel for a in attrs if a.rel is not None}
    return len(rels) >= 2


def rule_introduce_joins(node):

    if isinstance(node, ra.Select):
        # Önce alt ağaç içinde join’leri tanıt
        child = rule_introduce_joins(node.inputs[0])

        # Pattern: σ_c(R1 × R2) ve c join koşulu ise -> Join
        if isinstance(child, ra.Cross) and _is_join_condition(node.cond):
            left = rule_introduce_joins(child.inputs[0])
            right = rule_introduce_joins(child.inputs[1])
            # Koşulun kendisini bozmadan kullanıyoruz; sıralama korunuyor.
            return ra.Join(left, node.cond, right)
        else:
            return ra.Select(node.cond, child)

    if isinstance(node, ra.Project):
        return ra.Project(node.attrs, rule_introduce_joins(node.inputs[0]))
    if isinstance(node, ra.Rename):
        return ra.Rename(node.relname, node.attrnames,
                         rule_introduce_joins(node.inputs[0]))
    if isinstance(node, ra.Cross):
        return ra.Cross(rule_introduce_joins(node.inputs[0]),
                        rule_introduce_joins(node.inputs[1]))
    if isinstance(node, ra.Join):
        return ra.Join(rule_introduce_joins(node.inputs[0]),
                       node.cond,
                       rule_introduce_joins(node.inputs[1]))
    if isinstance(node, ra.SetOp):
        return type(node)(rule_introduce_joins(node.inputs[0]),
                          rule_introduce_joins(node.inputs[1]))
    return node
