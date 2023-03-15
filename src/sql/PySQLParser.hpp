//
// Created by cheesekun on 3/6/23.
//

#ifndef ULTRAVERSE_PYSQLPARSER_HPP
#define ULTRAVERSE_PYSQLPARSER_HPP

#include <string>
#include <mutex>
#include <memory>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

namespace ultraverse::sql {
    
    
    class PySQLParser {
    public:
        using PyObjectPtr = std::shared_ptr<PyObject>;
        
        static void initialize();
        static void finalize();
        
        PySQLParser();
        
        std::pair<std::string, uint64_t> getProcedureHint(const std::string &statement);
        
        bool queryMatches(const std::string &statementA, const std::string &statementB);
    private:
        static PyObject *loadParserScript();
        
        static std::once_flag _pyInitFlag;
        
        static PyObject *_sqlparseModule;
        static PyObject *_globalDict;
        static PyObject *_localDict;
    };
    
}


#endif //ULTRAVERSE_PYSQLPARSER_HPP
