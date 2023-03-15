# R"(

from typing import List, Optional

from sqlparse.sql import Statement, TokenList, Token, Values, Parenthesis, IdentifierList, Where, Comparison, Identifier
from sqlparse.tokens import Punctuation, Whitespace, String, Number, Operator

def get_values(statement: Statement) -> Parenthesis:
    values: Values = list(filter(lambda t: isinstance(t, Values), statement.tokens))[0]

    return values.tokens[-1]

def get_identifier_list(statement: Statement) -> Optional[IdentifierList]:
    identifier_list = list(filter(lambda t: isinstance(t, IdentifierList), statement.tokens))

    if len(identifier_list) != 0:
        return identifier_list[0]

    return None

def get_where(statement: Statement) -> Optional[Where]:
    where = list(filter(lambda t: isinstance(t, Where), statement.tokens))

    if len(where) != 0:
        return where[0]

    return None

def get_procedure_hint(sqlparse, statement_str: str) -> (str, int):
    statement: Statement = sqlparse.parse(statement_str)[0]
    tokens = list(statement.flatten())

    if len(list(filter(lambda t: t.match(sqlparse.tokens.Name, ('__ULTRAVERSE_PROCEDURE_HINT')), tokens))) == 0:
        return None

    values = get_values(statement)

    proc_name: Optional[str] = None
    call_id: Optional[int] = None

    for token in values.tokens:
        token: Token
        if token.ttype in String:
            proc_name = token.value
        if token.ttype in Number:
            call_id = token.value

    if (proc_name is not None) and (call_id is not None):
        return (proc_name, call_id)

    return None


def query_matches(sqlparse, statement_a_str: str, statement_b_str: str) -> bool:

    def get_table_name(statement: Statement) -> str:
        identifiers = list(filter(lambda t: isinstance(t, Identifier), statement.tokens))

        return str(identifiers[0])

    def query_type_matches(a: Statement, b: Statement) -> bool:
        query_type_a = str(a.tokens[0].value).upper()
        query_type_b = str(b.tokens[0].value).upper()

        return query_type_a == query_type_b

    def extract_comparisons(token_list: Optional[TokenList]) -> List[Comparison]:
        if token_list is None:
            return []

        return list(filter(lambda t: isinstance(t, Comparison), token_list.tokens))

    def comparison_matches(a: List[Comparison], b: List[Comparison]) -> bool:
        if len(a) != len(b):
            return False

        for i in range(0, len(a)):
            # FIXME: 연산자까지 같은지 비교하고 있지 않잖아.
            if str(a[i].left) != str(b[i].left):
                return False

        return True

    def match_select(a: Statement, b: Statement) -> bool:
        select_a = get_identifier_list(a)
        select_b = get_identifier_list(b)

        where_a = extract_comparisons(get_where(a))
        where_b = extract_comparisons(get_where(b))

        # FIXME: str() 대신 다른 방법 사용해야 함
        return str(select_a) == str(select_b) and comparison_matches(where_a, where_b)

    def match_insert(a: Statement, b: Statement) -> bool:
        values_a = get_values(statement_a)
        values_b = get_values(statement_b)

        parent_b = values_b.parent
        token_index_b = parent_b.token_index(values_b)

        parent_b.tokens[token_index_b] = values_a

        formatted_a = sqlparse.format(str(a), reindent=True, keyword_case='upper')
        formatted_b = sqlparse.format(str(b), reindent=True, keyword_case='upper')

        return formatted_a == formatted_b

    def match_update(a: Statement, b: Statement) -> bool:
        set_a = extract_comparisons(get_identifier_list(a))
        set_b = extract_comparisons(get_identifier_list(b))

        where_a = extract_comparisons(get_where(a))
        where_b = extract_comparisons(get_where(b))

        set_matches = comparison_matches(set_a, set_b)
        where_matches = comparison_matches(where_a, where_b)

        return set_matches and where_matches

    def match_delete(a: Statement, b: Statement) -> bool:
        where_a = extract_comparisons(get_where(a))
        where_b = extract_comparisons(get_where(b))

        return comparison_matches(where_a, where_b)

    statement_a: Statement = sqlparse.parse(statement_a_str)[0]
    statement_b: Statement = sqlparse.parse(statement_b_str)[0]

    if not query_type_matches(statement_a, statement_b):
        return False

    if get_table_name(statement_a) != get_table_name(statement_b):
        return False

    query_type = str(statement_a.tokens[0].value).upper()

    if query_type == 'SELECT':
        return match_select(statement_a, statement_b)
    elif query_type == 'INSERT':
        return match_insert(statement_a, statement_b)
    elif query_type == 'UPDATE':
        return match_update(statement_a, statement_b)
    elif query_type == 'DELETE':
        return match_delete(statement_a, statement_b)
    else:
        return statement_a_str == statement_b_str


# )"
