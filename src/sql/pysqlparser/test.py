from parser_main import query_matches

if __name__ == "__main__":
    import sqlparse

    def test_update():
        statement_a = 'UPDATE colors SET red = 0.5, blue = 1.0 WHERE id = 3'
        statement_b = 'UPDATE colors SET red = 7.5, blue = 2.3 WHERE id = 24'
        statement_c = 'UPDATE colors SET blue = 0.25, alpha = 1.2 WHERE id = 0.5'
        statement_d = 'UPDATE colors_2 SET red = 7.5, blue = 2.3 WHERE id = 24'

        # should print True
        print(query_matches(sqlparse, statement_a, statement_b))
        # should print False
        print(query_matches(sqlparse, statement_a, statement_c))
        # should print False
        print(query_matches(sqlparse, statement_a, statement_d))

    def test_select():
        statement_a = 'SELECT red, green, blue FROM colors WHERE id = 42'
        statement_b = 'SELECT red, green, blue FROM colors WHERE id = 24'
        statement_c = 'SELECT red, alpha, green FROM colors WHERE id = 24'

        # should print True
        print(query_matches(sqlparse, statement_a, statement_b))
        # should print False
        print(query_matches(sqlparse, statement_a, statement_c))


    def test_delete():
        statement_a = 'DELETE FROM colors WHERE id = 42'
        statement_b = 'DELETE FROM colors WHERE id = 24'
        statement_c = 'DELETE FROM colors WHERE red = 1.0'

        # should print True
        print(query_matches(sqlparse, statement_a, statement_b))
        # should print False
        print(query_matches(sqlparse, statement_a, statement_c))

    test_delete()
