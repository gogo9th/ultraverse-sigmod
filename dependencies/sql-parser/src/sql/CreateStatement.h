#ifndef SQLPARSER_CREATE_STATEMENT_H
#define SQLPARSER_CREATE_STATEMENT_H

#include "SQLStatement.h"

#include <ostream>

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {
  struct SelectStatement;

  enum class DataType {
    UNKNOWN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    CHAR,
    VARCHAR,
    TEXT
  };

  // Represents the type of a column, e.g., FLOAT or VARCHAR(10)
  struct ColumnType {
    ColumnType() = default;
    ColumnType(DataType data_type, int64_t length = 0);
    DataType data_type;
    int64_t length;  // Used for, e.g., VARCHAR(10)
  };

  bool operator==(const ColumnType& lhs, const ColumnType& rhs);
  bool operator!=(const ColumnType& lhs, const ColumnType& rhs);
  std::ostream& operator<<(std::ostream&, const ColumnType&);

  // Represents definition of a table column
  struct ColumnDefinition {
    ColumnDefinition(char* name, ColumnType type, bool nullable);
    virtual ~ColumnDefinition();

    char* name;
    ColumnType type;
    bool nullable;
  };

  enum CreateType {
    kCreateTable,
    kCreateTableFromTbl, // Hyrise file format
    kCreateView
  };

  // Represents SQL Create statements.
  // Example: "CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE)"
  struct CreateStatement : SQLStatement {
    CreateStatement(CreateType type);
    virtual ~CreateStatement();

    CreateType type;
    bool ifNotExists; // default: false
    char* filePath;   // default: nullptr
    int pos;
    char* schema;     // default: nullptr
    char* tableName;  // default: nullptr
    std::vector<ColumnDefinition*>* columns; // default: nullptr
    std::vector<char*>* viewColumns;
    SelectStatement* select;
  };

} // namespace hsql

#endif
