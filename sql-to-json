#! /usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
from   collections import OrderedDict
import json
import os
import sys

major, minor = sys.version_info[:2]
if not ((major == 2 and minor >= 7) or (major == 3 and minor >= 2)):
    sys.stderr.write(
        "This program requires Python 2.7+ or 3.3+\n"
        "You're trying to run it with Python {}.\n"
        .format('.'.join(map(str, sys.version_info[:3]))))
    sys.exit(1)

if major == 2:
    text = unicode
    ints = (int, long)
else:
    text = str
    ints = (int,)

def main():
    args = sys.argv[1:]
    if len(args) != 1:
        sys.stderr.write("Expecting exactly 1 command-line argument, got {}.\n".format(len(args)))
        sys.exit(1)

    fn = args[0]
    if not isinstance(fn, text):  # For Python 2
        fn = args[0].decode('utf-8')

    try:
        with open(fn, 'rb') as f:
            query_bytes = f.read();
    except IOError as e:
        sys.stderr.write("Unable to read from {}: {}\n".format(q(fn), e.strerror))
        sys.exit(1)

    try:
        query = query_bytes.decode('utf-8')
    except UnicodeDecodeError as e:
        sys.stderr.write(
            "Expected standard input to be valid UTF-8, but it wasn't.\n"
            "{}\n".format(e))
        sys.exit(1)

    try:
        tokens = list(tokenize(query))
        p = Parser(tokens);
        ast = p.p_select()
    except ParseError as e:
        sys.stderr.write("line {}, col {}: {}\n".format(e.pos[0], e.pos[1], e.message))
        sys.exit(1)

    dump(sys.stdout, ast)

def q(s):
    assert isinstance(s, text)
    return json.dumps(s)

class ParseError(Exception):
    def __init__(self, pos, message):
        self.pos = pos
        self.message = message

def typ_to_friendly(typ):
    if (typ in KEYWORDS) or (typ in OPS) or (typ in DELIMITERS):
        return '"' + typ + '"'
    elif typ == 'ident':
        return "identifier"
    elif typ == 'op':
        return "a comparison operator"
    elif typ == 'end':
        return "the end of input"
    elif typ == 'lit-str':
        return "a string literal"
    elif typ == 'lit-int':
        return "an integer literal"
    else:
        raise AssertionError("unknown typ: {!r}".format(typ))

def tok_to_friendly(t):
    typ = t.typ
    if (typ in KEYWORDS) or (typ in OPS) or (typ in DELIMITERS):
        return '"' + typ + '"'
    elif typ == 'ident':
        return "identifier \"{}\"".format(t.val)
    elif typ == 'op':
        return "\"{}\"".format(t.val)
    elif typ == 'end':
        return "the end of input"
    elif typ == 'lit-str':
        return "string literal \"{}\"".format(t.val)
    elif typ == 'lit-int':
        return "integer literal {}".format(t.val)
    else:
        raise AssertionError("unknown typ: {!r}".format(typ))

class Parser:
    def __init__(self, tokens):
        self._tokens = tokens
        self._i = 0

    def peek(self):
        return self._tokens[self._i]

    def check(self, typ):
        t = self._tokens[self._i]
        if t.typ != typ:
            return None
        self._i += 1
        return t

    def expect(self, expected_typ):
        t = self._tokens[self._i]
        if t.typ != expected_typ:
            raise self.fail(typ_to_friendly(expected_typ))
        self._i += 1
        return t

    def take(self):
        self._i += 1

    def fail(self, expected=None):
        t = self._tokens[self._i]
        got = tok_to_friendly(t)
        if expected is not None:
            return ParseError(t.pos, "expected {}, got {}".format(expected, got))
        else:
            return ParseError(t.pos, "not expecting {}".format(got))

    def p_select(self):
        self.expect('SELECT')
        selectors = self.p_separated(',', self.p_selector)

        self.expect('FROM')
        from_tables = self.p_separated(',', self.p_table_ref)

        where_clauses = []
        if self.check('WHERE'):
            where_clauses = self.p_separated('AND', self.p_comparison)

        #group_by_columns = []
        #if self.check('GROUP'):
        #    self.expect('BY')
        #    group_by_columns = list(map(Line, self.p_separated(',', self.p_column_ref)))

        if self.check('end'):
            return OrderedDict((
                ('select', selectors),
                ('from', from_tables),
                ('where', where_clauses),
        #        ('group_by', group_by_columns),
            ))

        raise self.fail()

    def p_selector(self):
        source = self.p_selector_source()
        rename = None
        if self.check('AS'):
            rename = self.expect('ident').val
        return OrderedDict((
            ('source', source),
            ('as', rename),
        ))

    def p_selector_source(self):
        ok, v = self.p_maybe_column_ref()
        if ok:
            return Line({'column': v})
        #if self.check('COUNT'):
        #    return Line({'count': None})
        #if self.check('SUM'):
        #    self.expect('(')
        #    arg = self.p_column_ref()
        #    self.expect(')')
        #    return Line({'sum': arg})
        raise self.fail("a list of fields to select")

    def p_column_ref(self):
        ok, v = self.p_maybe_column_ref()
        if not ok:
            raise self.fail(v)
        return v

    def p_maybe_column_ref(self):
        t = self.check('ident')
        if t is None:
            return False, typ_to_friendly('ident')
        if self.check('.'):
            table = t.val
            name = self.p_ident()
        else:
            table = None
            name = t.val
        return True, OrderedDict((
            ('name', name),
            ('table', table),
        ))

    def p_table_ref(self):
        name = self.p_ident()
        rename = None
        if self.check('AS'):
            rename = self.p_ident()
        return OrderedDict((
            ('source', Line({'file': name})),
            ('as', rename),
        ))

    def p_ident(self):
        return self.expect('ident').val

    def p_comparison(self):
        left = self.p_term()
        op = self.expect('op').val
        right = self.p_term()
        return OrderedDict((
            ('op', op),
            ('left', left),
            ('right', right),
        ))

    def p_term(self):
        ok, v = self.p_maybe_column_ref()
        if ok:
            return Line({'column': v})
        t = self.peek()
        if t.typ == 'lit-str':
            self.take()
            return Line({'lit_str': t.val})
        if t.typ == 'lit-int':
            self.take()
            return Line({'lit_int': t.val})
        raise self.fail("a column reference or a string/integer literal")

    def p_separated(self, separator_typ, parse):
        l = [parse()]
        while self.check(separator_typ):
            l.append(parse())
        return l

INT_LIT_MAX = (2 ** 31) - 1

OP_CHARS = frozenset(('=', '!', '<', '>'))
OPS = frozenset(('=', '!=', '>', '>=', '<', '<='))

KEYWORDS = frozenset(('SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'COUNT', 'SUM', 'AS', 'AND'))

DELIMITERS = frozenset(('.', ',', '(', ')'))

def isprint(c):
    return 32 <= ord(c) <= 126

def tokenize(query):
    assert isinstance(query, text)
    i = 0

    line = 1
    col_start = 0

    def pos(index):
        col = (index - col_start) + 1
        return (line, col)

    def tok(index, typ, val=None):
        return Token(pos(index), typ, val)

    def err(index, message):
        col = (i - col_start) + 1
        return ParseError(pos(index), message)

    while True:
        if i >= len(query):
            yield tok(i, 'end')
            return

        c = query[i]

        if c == ' ':
            i += 1

        # "--" comemnt
        elif c == '-':
            i += 1
            if i >= len(query) or query[i] != '-':
                raise err(i-1, "unexpected \"-\" (use \"--\" for comments)")
            while True:
                i += 1
                if i >= len(query):
                    break
                c = query[i]
                if c == '\n':
                    break

        elif c == '\n':
            i += 1
            line += 1
            col_start = i

        elif c == '\r':
            # TODO: Ensure that it's followed by a "\n".
            i += 1

        elif text.isdigit(c):
            start = i
            while True:
                i += 1
                if i >= len(query):
                    break;
                c = query[i]
                if text.isalpha(c) or c == '_':
                    raise err(i, "invalid character suffixed on to integer literal")
                if not text.isdigit(c):
                    break
            v = int(query[start:i])
            if v > INT_LIT_MAX:
                raise err(start, "integer literal too large: {} (max allowed: {})".format(v, INT_LIT_MAX))
            yield tok(start, 'lit-int', v)

        elif c == '"':
            start = i
            while True:
                i += 1
                if i >= len(query):
                    raise err(start, "string literal goes unterminated to end of input")
                c = query[i]
                if c == '\n':
                    raise err(start, "string literal goes unterminated to end of line")
                if c == '"':
                    i += 1
                    break
                if not isprint(c):
                    raise err(i, "invalid character in string literal: {}".format(q(c)))
            v = query[start+1:i-1]
            yield tok(start, 'lit-str', v)

        elif c in DELIMITERS:
            yield tok(i, c)
            i += 1

        elif c in OP_CHARS:
            start = i
            while True:
                i += 1
                if i >= len(query):
                    break;
                c = query[i]
                if c not in OP_CHARS:
                    break
            op = query[start:i]
            if op not in ('=', '!=', '>', '>=', '<', '<='):
                raise err(start, "invalid operator: {}".format(q(op)))
            yield tok(start, 'op', op)

        elif text.isalnum(c) or c == '_':
            start = i
            while True:
                i += 1
                if i >= len(query):
                    break;
                c = query[i]
                if not (text.isalnum(c) or c == '_'):
                    break
            v = query[start:i]
            if v in KEYWORDS:
                yield tok(start, v)
            elif text.isupper(v[0]):
                raise err(start, "invalid token: {}; it's not a keyword but it starts with an upper-case letter; only keywords can start with an upper-case letter".format(q(v)))
            else:
                yield tok(start, 'ident', v)

        else:
            raise err(i, "unexpected character {}".format(q(c)))

class Token:
    def __init__(self, pos, typ, val=None):
        self.pos = pos
        self.typ = typ
        self.val = val

    def __repr__(self):
        if self.val is None:
            return 'Token({!r}, {!r})'.format(self.pos, self.typ)
        else:
            return 'Token({!r}, {!r}, {!r})'.format(self.pos, self.typ, self.val)

# A formatting marker.  It means to render the contents on a single line.
class Line:
    def __init__(self, inner):
        self.inner = inner

def dump(f, obj):
    _dump('', f, obj)
    f.write('\n')

def _dump(prefix, f, obj):
    if isinstance(obj, ints):
        json.dump(obj, f)
    elif isinstance(obj, (text, bool, type(None))):
        json.dump(obj, f)
    elif isinstance(obj, Line):
        json.dump(obj.inner, f)
    elif isinstance(obj, dict):
        if len(obj) == 0:
            f.write('{}')
        else:
            sub_prefix = prefix + '    '
            f.write('{')
            sep = '\n'
            for k, v in obj.items():
                f.write(sep); sep = ',\n'
                f.write(sub_prefix)
                json.dump(k, f)
                f.write(': ')
                _dump(sub_prefix, f, v)
            f.write('\n')
            f.write(prefix + '}')
    elif isinstance(obj, (list, tuple)):
        if len(obj) == 0:
            f.write('[]')
            return
        else:
            sub_prefix = prefix + '    '
            f.write('[')
            sep = '\n'
            for e in obj:
                f.write(sep); sep = ',\n'
                f.write(sub_prefix)
                _dump(sub_prefix, f, e)
            f.write('\n')
            f.write(prefix + ']')
    else:
        raise AssertionError("unhandled: {!r}".format(obj))

if __name__ == '__main__':
    main()
