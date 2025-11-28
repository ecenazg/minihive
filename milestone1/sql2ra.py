#Ecenaz Güngör - Matriculation number: 118802 

from sqlparse.sql import Identifier, IdentifierList, Where, Comparison 
from sqlparse.tokens import Keyword, DML, Wildcard, Number, String, Operator 

from radb import ast as raAst
sym = raAst.sym

def parseAttribute(token: Identifier):
    table = token.get_parent_name()
    column = token.get_real_name()
    return raAst.AttrRef(table, column)

def extractSelect(stmt):
   
    columns = []
    selectPart = False
    firstSelect = False

    for token in stmt.tokens:
        if token.ttype is DML and token.value.upper() == "SELECT":
            firstSelect = True
            continue

        if not firstSelect:
            continue

        if token.is_whitespace:
            continue

        if token.ttype is Keyword and token.value.upper() in ("DISTINCT",):
            continue

        if token.ttype is Keyword and token.value.upper() == "FROM":
            break


        if isinstance(token, IdentifierList):
            for name in token.get_identifiers():
                columns.append(parseAttribute(name))
            continue


        if isinstance(token, Identifier):
            columns.append(parseAttribute(token))
            continue


        if token.ttype is Wildcard:
            selectPart = True
            break

    return selectPart, columns

def extractFrom(stmt):
    tables = []
    fromFlag = False

    for token in stmt.tokens:
        if token.is_whitespace:
            continue

        if token.ttype is DML and token.value.upper() == "SELECT":
            continue

        if token.ttype is Keyword and token.value.upper() == "FROM":
            fromFlag = True
            continue

        if not fromFlag:
            continue

        if isinstance(token, Where):
            break

        if isinstance(token, IdentifierList):
            for item in token.get_identifiers():
                tables.append((item.get_real_name(), item.get_alias()))
            continue

        if isinstance(token, Identifier):
            tables.append((token.get_real_name(), token.get_alias()))
            continue

    return tables

def joins(tables):
    if not tables:
        return None

    relation = []
    for tableName, alias in tables:
        base = raAst.RelRef(tableName)
        if alias:
            base = raAst.Rename(alias, None, base)
        relation.append(base)

    result = relation[0]
    for nextTable in relation[1:]:
        result = raAst.Cross(result, nextTable)

    return result


def changeToRA(token):
    if isinstance(token, Identifier):
        return parseAttribute(token)
    if token.ttype in Number:
        return raAst.RANumber(token.value)
    if token.ttype in String:
        return raAst.RAString(token.value)
    return raAst.RAString(token.value)


def buildComparison(comp: Comparison):
    left, right = None, None
    opt = None
    optFlag = False

    for tok in comp.tokens:
        if tok.is_whitespace:
            continue
        if tok.ttype is Operator.Comparison or tok.value in ("=", ">", "<", ">=", "<=", "<>"):
            opt = tok.value
            optFlag = True
            continue
        if not optFlag:
            left = tok
        else:
            right = tok

    optMap = {
    "=": sym.EQ,
    "<": sym.LT,
    ">": sym.GT,
    "<=": sym.LE,
    ">=": sym.GE,
    "<>": sym.NE,
    "!=": sym.NE,
}

    if opt not in optMap:
        raise ValueError(f"Unknown comparison operator: {opt}")
    optSym = optMap[opt]

    return raAst.ValExprBinaryOp(
        changeToRA(left), optSym, changeToRA(right)
    )

def extractWhere(stmt):
    where = None
    for token in stmt.tokens:
        if isinstance(token, Where):
            where = token
            break

    if not where:
        return None

    comparisons = [t for t in where.tokens if isinstance(t, Comparison)]
    if not comparisons:
        return None

    condition = buildComparison(comparisons[0])
    for extra in comparisons[1:]:
        condition = raAst.ValExprBinaryOp(condition, sym.AND, buildComparison(extra))

    return condition


def translate(stmt):
    # FROM
    tables = extractFrom(stmt)
    if not tables:
        raise ValueError("No tables found in FROM clause.")
    raExpr = joins(tables)

    # WHERE
    condition = extractWhere(stmt)
    if condition is not None:
        raExpr = raAst.Select(condition, raExpr)

    # SELECT
    selectPart, columns = extractSelect(stmt)
    if not selectPart:
        raExpr = raAst.Project(columns, raExpr)

    return raExpr
