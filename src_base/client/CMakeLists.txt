# Copyright (c) 2006, 2015, Oracle and/or its affiliates. All rights reserved.
# Copyright (c) 2008, 2019, MariaDB Corporation
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/include
  ${PCRE_INCLUDES}
  ${CMAKE_SOURCE_DIR}/mysys_ssl
  ${ZLIB_INCLUDE_DIR}
  ${SSL_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/sql
  ${CMAKE_SOURCE_DIR}/strings
  ${MY_READLINE_INCLUDE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

INCLUDE_DIRECTORIES(BEFORE
  ${CMAKE_BINARY_DIR}/libmariadb/include
  ${CMAKE_SOURCE_DIR}/libmariadb/include)

## We will need libeay32.dll and ssleay32.dll when running client executables.
COPY_OPENSSL_DLLS(copy_openssl_client)

SET(CLIENT_LIB mariadbclient mysys)

ADD_DEFINITIONS(${SSL_DEFINES})
MYSQL_ADD_EXECUTABLE(mysql completion_hash.cc mysql.cc readline.cc
                           ${CMAKE_SOURCE_DIR}/sql/sql_string.cc)
TARGET_LINK_LIBRARIES(mysql ${CLIENT_LIB})
IF(UNIX)
  TARGET_LINK_LIBRARIES(mysql ${MY_READLINE_LIBRARY})
  SET_TARGET_PROPERTIES(mysql PROPERTIES ENABLE_EXPORTS TRUE)
ENDIF(UNIX)

MYSQL_ADD_EXECUTABLE(mysqltest mysqltest.cc COMPONENT Test)
SET_SOURCE_FILES_PROPERTIES(mysqltest.cc PROPERTIES COMPILE_FLAGS "-DTHREADS")
TARGET_LINK_LIBRARIES(mysqltest  ${CLIENT_LIB} pcreposix pcre)
SET_TARGET_PROPERTIES(mysqltest PROPERTIES ENABLE_EXPORTS TRUE)


MYSQL_ADD_EXECUTABLE(mysqlcheck mysqlcheck.c)
TARGET_LINK_LIBRARIES(mysqlcheck ${CLIENT_LIB})

MYSQL_ADD_EXECUTABLE(mysqldump mysqldump.c ../sql-common/my_user.c)
TARGET_LINK_LIBRARIES(mysqldump ${CLIENT_LIB})


MYSQL_ADD_EXECUTABLE(mysqlimport mysqlimport.c)
SET_SOURCE_FILES_PROPERTIES(mysqlimport.c PROPERTIES COMPILE_FLAGS "-DTHREADS")
TARGET_LINK_LIBRARIES(mysqlimport ${CLIENT_LIB})

MYSQL_ADD_EXECUTABLE(mysql_upgrade mysql_upgrade.c COMPONENT Server)
TARGET_LINK_LIBRARIES(mysql_upgrade ${CLIENT_LIB})
ADD_DEPENDENCIES(mysql_upgrade GenFixPrivs)

MYSQL_ADD_EXECUTABLE(mysqlshow mysqlshow.c)
TARGET_LINK_LIBRARIES(mysqlshow ${CLIENT_LIB})

MYSQL_ADD_EXECUTABLE(mysql_plugin mysql_plugin.c)
TARGET_LINK_LIBRARIES(mysql_plugin ${CLIENT_LIB})

MYSQL_ADD_EXECUTABLE(mysqlbinlog mysqlbinlog.cc)
TARGET_LINK_LIBRARIES(mysqlbinlog ${CLIENT_LIB})

find_package(BISON)
find_package(FLEX)

BISON_TARGET(sqlparser_bison
  sql-parser/src/parser/bison_parser.y
  ${CMAKE_CURRENT_BINARY_DIR}/bison_parser.cpp
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/bison_parser.h)
FLEX_TARGET(sqlparser_flex
  sql-parser/src/parser/flex_lexer.l
  ${CMAKE_CURRENT_BINARY_DIR}/flex_lexer.cpp
  COMPILE_FLAGS --header-file=${CMAKE_CURRENT_BINARY_DIR}/flex_lexer.h)

ADD_FLEX_BISON_DEPENDENCY(sqlparser_flex sqlparser_bison)

ADD_LIBRARY(sqlparser STATIC
  sql-parser/src/SQLParser.cpp
  sql-parser/src/SQLParserResult.cpp
  sql-parser/src/util/sqlhelper.cpp
  sql-parser/src/sql/PrepareStatement.cpp
  sql-parser/src/sql/Expr.cpp
  sql-parser/src/sql/SQLStatement.cpp
  sql-parser/src/sql/statements.cpp
  ${BISON_sqlparser_bison_OUTPUTS}
  ${FLEX_sqlparser_flex_OUTPUTS}
)
TARGET_INCLUDE_DIRECTORIES(sqlparser PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
TARGET_INCLUDE_DIRECTORIES(sqlparser PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/sql-parser/src/parser)

FIND_PACKAGE(PkgConfig REQUIRED)
pkg_check_modules(GVC REQUIRED libgvc)
MYSQL_ADD_EXECUTABLE(db_state_change ../../src/mariadb/state/db_state_change.cc StateUserQuery.cc StateGraphGvc.cc StateGraphBoost.cc StateTable.cc StateBinaryLog.cc StateItem.cc StateForeign.cc)
TARGET_LINK_LIBRARIES(db_state_change ${CLIENT_LIB} ${GVC_LIBRARIES} sqlparser)
TARGET_INCLUDE_DIRECTORIES(db_state_change PUBLIC ${GVC_INCLUDE_DIRS})
TARGET_INCLUDE_DIRECTORIES(db_state_change PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/sql-parser/src)
TARGET_COMPILE_OPTIONS(db_state_change PRIVATE -O2 -g -frtti)

find_package(Boost COMPONENTS program_options REQUIRED)
MYSQL_ADD_EXECUTABLE(query_analyzer query_analyzer.cc StateUserQuery.cc StateGraphGvc.cc StateGraphBoost.cc StateTable.cc StateBinaryLog.cc StateItem.cc StateForeign.cc)
execute_process(COMMAND git log -1 --pretty=%h OUTPUT_VARIABLE QUERY_ANALYZER_VERSION)
TARGET_COMPILE_DEFINITIONS(query_analyzer PRIVATE QUERY_ANALYZER_VERSION=${QUERY_ANALYZER_VERSION})
TARGET_LINK_LIBRARIES(query_analyzer ${Boost_LIBRARIES})
TARGET_LINK_LIBRARIES(query_analyzer ${CLIENT_LIB} ${GVC_LIBRARIES} sqlparser)
TARGET_INCLUDE_DIRECTORIES(query_analyzer PUBLIC ${Boost_INCLUDE_DIR})
TARGET_INCLUDE_DIRECTORIES(query_analyzer PUBLIC ${GVC_INCLUDE_DIRS})
TARGET_INCLUDE_DIRECTORIES(query_analyzer PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/sql-parser/src)
TARGET_COMPILE_OPTIONS(query_analyzer PRIVATE -O2 -g -frtti)

MYSQL_ADD_EXECUTABLE(state_log_viewer state_log_viewer.cc StateUserQuery.cc StateGraphGvc.cc StateGraphBoost.cc StateTable.cc StateBinaryLog.cc StateItem.cc StateForeign.cc)
TARGET_LINK_LIBRARIES(state_log_viewer ${CLIENT_LIB} ${GVC_LIBRARIES} sqlparser)
TARGET_INCLUDE_DIRECTORIES(state_log_viewer PUBLIC ${GVC_INCLUDE_DIRS})
TARGET_INCLUDE_DIRECTORIES(state_log_viewer PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/sql-parser/src)
TARGET_COMPILE_OPTIONS(state_log_viewer PRIVATE -O2 -g -frtti)

MYSQL_ADD_EXECUTABLE(mysqladmin mysqladmin.cc ../sql/password.c)
TARGET_LINK_LIBRARIES(mysqladmin ${CLIENT_LIB})

MYSQL_ADD_EXECUTABLE(mysqlslap mysqlslap.c)
SET_SOURCE_FILES_PROPERTIES(mysqlslap.c PROPERTIES COMPILE_FLAGS "-DTHREADS")
TARGET_LINK_LIBRARIES(mysqlslap ${CLIENT_LIB})

# "WIN32" also covers 64 bit. "echo" is used in some files below "mysql-test/".
IF(WIN32)
  MYSQL_ADD_EXECUTABLE(echo echo.c COMPONENT Junk)
ENDIF(WIN32)

# async_example is just a code example, do not install it.
ADD_EXECUTABLE(async_example async_example.c)
TARGET_LINK_LIBRARIES(async_example ${CLIENT_LIB})

SET_TARGET_PROPERTIES (mysqlcheck mysqldump mysqlimport mysql_upgrade mysqlshow mysqlslap mysql_plugin async_example
PROPERTIES HAS_CXX TRUE)


FOREACH(t mysql mysqltest mysqltest mysqlcheck mysqldump mysqlimport mysql_upgrade mysqlshow mysql_plugin mysqlbinlog
  mysqladmin mysqlslap async_example)
  ADD_DEPENDENCIES(${t} GenError ${CLIENT_LIB})
ENDFOREACH()

ADD_DEFINITIONS(-DHAVE_DLOPEN)

