project(sql-parser)

BISON_TARGET(sqlparser_bison
    src/parser/bison_parser.y
    ${CMAKE_CURRENT_BINARY_DIR}/bison_parser.cpp
    DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/bison_parser.h)
FLEX_TARGET(sqlparser_flex
    src/parser/flex_lexer.l
    ${CMAKE_CURRENT_BINARY_DIR}/flex_lexer.cpp
    COMPILE_FLAGS --header-file=${CMAKE_CURRENT_BINARY_DIR}/flex_lexer.h)

ADD_FLEX_BISON_DEPENDENCY(sqlparser_flex sqlparser_bison)

ADD_LIBRARY(sqlparser STATIC
    src/SQLParser.cpp
    src/SQLParserResult.cpp
    src/util/sqlhelper.cpp
    src/sql/PrepareStatement.cpp
    src/sql/Expr.cpp
    src/sql/SQLStatement.cpp
    src/sql/statements.cpp
    ${BISON_sqlparser_bison_OUTPUTS}
    ${FLEX_sqlparser_flex_OUTPUTS}
    )

target_include_directories(sqlparser PRIVATE src)
target_include_directories(sqlparser PRIVATE src/parser)
target_include_directories(sqlparser PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
