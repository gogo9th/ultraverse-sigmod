R"(

from typing import List, Optional

from sqlparse.sql import Statement, TokenList, Token, Values, Parenthesis, IdentifierList, Where, Comparison, Identifier
from sqlparse.tokens import Punctuation, Whitespace, String, Number, Operator


def query_matches(sqlparse, statement_a_str: str, statement_b_str: str) -> bool:

    def get_values(statement):
        values = list(filter(lambda t: isinstance(t, sqlparse.sql.Values), statement.tokens))[0]

        return values.tokens[-1]

    def get_identifier_list(statement):
        identifier_list = list(filter(lambda t: isinstance(t, sqlparse.sql.IdentifierList), statement.tokens))

        if len(identifier_list) != 0:
            return identifier_list[0]

        return None

    def get_where(statement):
        where = list(filter(lambda t: isinstance(t, sqlparse.sql.Where), statement.tokens))

        if len(where) != 0:
            return where[0]

        return None
    def get_table_name(statement) -> str:
        identifiers = list(filter(lambda t: isinstance(t, sqlparse.sql.Identifier), statement.tokens))

        if len(identifiers) > 0:
            return str(identifiers[0])
        else:
            return 'UNKNOWN_TABLE'

    def query_type_matches(a, b) -> bool:
        query_type_a = str(a.tokens[0].value).upper()
        query_type_b = str(b.tokens[0].value).upper()

        return query_type_a == query_type_b

    def extract_comparisons(token_list):
        if token_list is None:
            return []

        return list(filter(lambda t: isinstance(t, sqlparse.sql.Comparison), token_list.tokens))

    def comparison_matches(a, b) -> bool:
        if len(a) != len(b):
            return False

        for i in range(0, len(a)):
            # FIXME: 연산자까지 같은지 비교하고 있지 않잖아.
            if str(a[i].left) != str(b[i].left):
                return False

        return True

    def match_select(a, b) -> bool:
        select_a = get_identifier_list(a)
        select_b = get_identifier_list(b)

        where_a = extract_comparisons(get_where(a))
        where_b = extract_comparisons(get_where(b))

        # FIXME: str() 대신 다른 방법 사용해야 함
        return str(select_a) == str(select_b) and comparison_matches(where_a, where_b)

    def match_insert(a, b) -> bool:
        values_a = get_values(statement_a)
        values_b = get_values(statement_b)

        parent_b = values_b.parent
        token_index_b = parent_b.token_index(values_b)

        parent_b.tokens[token_index_b] = values_a

        formatted_a = sqlparse.format(str(a), reindent=True, keyword_case='upper')
        formatted_b = sqlparse.format(str(b), reindent=True, keyword_case='upper')

        return formatted_a == formatted_b

    def match_update(a, b) -> bool:
        set_a = extract_comparisons(get_identifier_list(a))
        set_b = extract_comparisons(get_identifier_list(b))

        where_a = extract_comparisons(get_where(a))
        where_b = extract_comparisons(get_where(b))

        set_matches = comparison_matches(set_a, set_b)
        where_matches = comparison_matches(where_a, where_b)

        return set_matches and where_matches

    def match_delete(a, b) -> bool:
        where_a = extract_comparisons(get_where(a))
        where_b = extract_comparisons(get_where(b))

        return comparison_matches(where_a, where_b)

    statement_a = sqlparse.parse(statement_a_str)[0]
    statement_b = sqlparse.parse(statement_b_str)[0]

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


)"
