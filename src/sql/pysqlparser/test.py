R"(

from typing import List

from sqlparse.sql import Statement, TokenList, Token, Values, Parenthesis
from sqlparse.tokens import Punctuation, Whitespace, String

def get_values(statement: Statement) -> Parenthesis:
    values: Values = list(filter(lambda t: isinstance(t, Values), statement.tokens))[0]

    return values.tokens[-1]
def match_procedure_statements(sqlparse, procedure, binlog_statement: Statement):
    pass


def get_procedure_hint(sqlparse, statement_str: str) -> (str, int):
    statement: Statement = sqlparse.parse(statement_str)[0]
    tokens = list(statement.flatten())

    if len(list(filter(lambda t: t.match(sqlparse.tokens.Name, ('__ULTRAVERSE_PROCEDURE_HINT')), tokens))) == 0:
        return None

    values = get_values(statement)

    for token in values.tokens:
        token: Token
        if token.ttype in String:
            return token

    return None


# if __name__ == "__main__":
#     import sqlparse
#     statement: Statement = sqlparse.parse("INSERT INTO table1 (col1, col2, col3) VALUES ('text', (123 + 235), 456.5)")[0]
#     print(get_values(statement))
#
#     print(get_procedure_hint(sqlparse, "INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (procname) VALUES ('HELOWRLD');"))

)"
