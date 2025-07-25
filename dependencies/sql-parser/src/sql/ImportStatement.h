#ifndef SQLPARSER_IMPORT_STATEMENT_H
#define SQLPARSER_IMPORT_STATEMENT_H

#include "SQLStatement.h"

namespace hsql {
  enum ImportType {
    kImportCSV,
    kImportTbl, // Hyrise file format
  };

  // Represents SQL Import statements.
  struct ImportStatement : SQLStatement {
    ImportStatement(ImportType type);
    virtual ~ImportStatement();

    ImportType type;
    char* filePath;
    int pos;
    char* schema;
    char* tableName;
  };

} // namespace hsql

#endif
