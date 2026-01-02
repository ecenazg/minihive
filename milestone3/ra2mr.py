from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget

import radb
import radb.ast
import radb.parse

class ExecEnv(Enum):
    LOCAL = 1
    HDFS = 2
    MOCK = 3


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, filename):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(filename)
        if self.exec_environment == ExecEnv.MOCK:
            return MockTarget(filename)
        return luigi.LocalTarget(filename)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        # Input files are already in MR format:  RelName \t JSON
        return self.get_output(self.filename)



def count_steps(node):
    if isinstance(node, (radb.ast.Select, radb.ast.Project, radb.ast.Rename)):
        return 1 + count_steps(node.inputs[0])
    if isinstance(node, radb.ast.Join):
        return 1 + count_steps(node.inputs[0]) + count_steps(node.inputs[1])
    if isinstance(node, radb.ast.RelRef):
        return 1
    raise RuntimeError("Unsupported relational algebra node")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    querystring = luigi.Parameter()
    step = luigi.IntParameter(default=1)

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)



def task_factory(node, step=1, env=ExecEnv.HDFS):
    if isinstance(node, radb.ast.Select):
        return SelectTask(querystring=str(node) + ";", step=step, exec_environment=env)
    if isinstance(node, radb.ast.Project):
        return ProjectTask(querystring=str(node) + ";", step=step, exec_environment=env)
    if isinstance(node, radb.ast.Rename):
        return RenameTask(querystring=str(node) + ";", step=step, exec_environment=env)
    if isinstance(node, radb.ast.Join):
        return JoinTask(querystring=str(node) + ";", step=step, exec_environment=env)
    if isinstance(node, radb.ast.RelRef):
        return InputData(filename=node.rel + ".json", exec_environment=env)
    raise RuntimeError("Unsupported operator in task factory")


def _read_relation_tuples(target):
    rows = []
    with target.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rel, payload = line.split("\t", 1)
            rows.append((rel, json.loads(payload)))
    return rows


def _write_relation_tuples(target, rows):
    # IMPORTANT for MockTarget: ensure the file key exists even if empty
    with target.open("w") as f:
        wrote = False
        for rel, tup in rows:
            f.write(f"{rel}\t{json.dumps(tup)}\n")
            wrote = True
        if not wrote:
            f.write("")



def _resolve_attr(attr, tup):
    if getattr(attr, "rel", None):
        return tup.get(f"{attr.rel}.{attr.name}")

    if attr.name in tup:
        return tup[attr.name]

    suffix = f".{attr.name}"
    matches = [k for k in tup if k.endswith(suffix)]
    if len(matches) == 1:
        return tup[matches[0]]

    return None


def _projection_key(attr, tup):
    if getattr(attr, "rel", None):
        return f"{attr.rel}.{attr.name}"

    suffix = f".{attr.name}"
    matches = [k for k in tup if k.endswith(suffix)]
    if len(matches) == 1:
        return matches[0]

    return attr.name



_OP_MAP = {
    "43": "==",
    "=": "==",
    "==": "==",
    "!=": "!=",
    "<>": "!=",
    "<": "<",
    "<=": "<=",
    ">": ">",
    ">=": ">=",
}


def _strip_quotes(val):
    if isinstance(val, str) and len(val) >= 2:
        if (val[0] == val[-1]) and val[0] in ("'", '"'):
            return val[1:-1]
    return val


def _coerce(val):
    if isinstance(val, (int, float)):
        return val
    if not isinstance(val, str):
        return val

    s = val.strip()
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        try:
            return int(s)
        except Exception:
            return val
    try:
        return float(s)
    except Exception:
        return val


def _compare(lv, op, rv):
    lv = _coerce(_strip_quotes(lv))
    rv = _coerce(_strip_quotes(rv))

    if op == "==":
        return lv == rv
    if op == "!=":
        return lv != rv
    if op == "<":
        return lv < rv
    if op == "<=":
        return lv <= rv
    if op == ">":
        return lv > rv
    if op == ">=":
        return lv >= rv
    return False


def eval_pred(expr, tup):
    if expr is None:
        return True

    if hasattr(radb.ast, "ValExprParen") and isinstance(expr, radb.ast.ValExprParen):
        return eval_pred(expr.inputs[0], tup)

    if isinstance(expr, radb.ast.ValExprBinaryOp):
        left, right = expr.inputs
        raw_op = str(expr.op).strip()

        if raw_op.lower() == "and" or (raw_op.isdigit() and raw_op not in _OP_MAP):
            return eval_pred(left, tup) and eval_pred(right, tup)

        op = _OP_MAP.get(raw_op, raw_op)

        def value(x):
            if isinstance(x, radb.ast.AttrRef):
                return _resolve_attr(x, tup)
            return getattr(x, "val", x)

        lv = value(left)
        rv = value(right)

        if lv is None or rv is None:
            return False

        return _compare(lv, op, rv)

    return True



class SelectTask(RelAlgQueryTask):
    def requires(self):
        q = radb.parse.one_statement_from_string(self.querystring)
        return [task_factory(q.inputs[0], self.step + 1, self.exec_environment)]

    def run(self):
        if self.exec_environment in (ExecEnv.MOCK, ExecEnv.LOCAL):
            try:
                q = radb.parse.one_statement_from_string(self.querystring)
                rows = _read_relation_tuples(self.input()[0])
                out = [(r, t) for r, t in rows if eval_pred(q.cond, t)]
                _write_relation_tuples(self.output(), out)
            except Exception:
                _write_relation_tuples(self.output(), [])
                raise
            return
        super().run()


class ProjectTask(RelAlgQueryTask):
    def requires(self):
        q = radb.parse.one_statement_from_string(self.querystring)
        return [task_factory(q.inputs[0], self.step + 1, self.exec_environment)]

    def run(self):
        if self.exec_environment in (ExecEnv.MOCK, ExecEnv.LOCAL):
            try:
                q = radb.parse.one_statement_from_string(self.querystring)
                rows = _read_relation_tuples(self.input()[0])

                seen = set()
                out = []
                for rel, tup in rows:
                    proj = {}
                    for a in q.attrs:
                        key = _projection_key(a, tup)
                        val = _resolve_attr(a, tup)
                        if val is not None:
                            proj[key] = val

                    sig = json.dumps(proj, sort_keys=True)
                    if sig not in seen:
                        seen.add(sig)
                        out.append((rel, proj))

                _write_relation_tuples(self.output(), out)
            except Exception:
                _write_relation_tuples(self.output(), [])
                raise
            return
        super().run()


class RenameTask(RelAlgQueryTask):
    def requires(self):
        q = radb.parse.one_statement_from_string(self.querystring)
        return [task_factory(q.inputs[0], self.step + 1, self.exec_environment)]

    def run(self):
        if self.exec_environment in (ExecEnv.MOCK, ExecEnv.LOCAL):
            try:
                q = radb.parse.one_statement_from_string(self.querystring)
                rows = _read_relation_tuples(self.input()[0])
                out = []
                for _, tup in rows:
                    renamed = {}
                    for k, v in tup.items():
                        renamed[f"{q.relname}.{k.split('.')[-1]}"] = v
                    out.append((q.relname, renamed))
                _write_relation_tuples(self.output(), out)
            except Exception:
                _write_relation_tuples(self.output(), [])
                raise
            return
        super().run()


class JoinTask(RelAlgQueryTask):
    def requires(self):
        q = radb.parse.one_statement_from_string(self.querystring)
        left = task_factory(q.inputs[0], self.step + 1, self.exec_environment)
        right = task_factory(q.inputs[1], self.step + count_steps(q.inputs[0]) + 1, self.exec_environment)
        return [left, right]

    def run(self):
        if self.exec_environment in (ExecEnv.MOCK, ExecEnv.LOCAL):
            try:
                q = radb.parse.one_statement_from_string(self.querystring)
                left_rows = _read_relation_tuples(self.input()[0])
                right_rows = _read_relation_tuples(self.input()[1])

                out = []
                for _, lt in left_rows:
                    for _, rt in right_rows:
                        merged = dict(lt)
                        merged.update(rt)
                        if eval_pred(q.cond, merged):
                            out.append(("Join", merged))

                _write_relation_tuples(self.output(), out)
            except Exception:
                _write_relation_tuples(self.output(), [])
                raise
            return
        super().run()


if __name__ == "__main__":
    luigi.run()
