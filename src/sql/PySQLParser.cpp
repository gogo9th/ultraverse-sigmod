//
// Created by cheesekun on 3/6/23.
//

#include <iostream>

#include "PySQLParser.hpp"

namespace ultraverse::sql {
    std::once_flag PySQLParser::_pyInitFlag;
    
    PyObject *PySQLParser::_sqlparseModule;
    PyObject *PySQLParser::_globalDict;
    PyObject *PySQLParser::_localDict;
    
    void PySQLParser::initialize() {
        std::call_once(_pyInitFlag, []() {
            wchar_t *programName = Py_DecodeLocale("ultinernal-pysqlparser", nullptr);
            
            Py_SetProgramName(programName);
            Py_Initialize();
            
            _globalDict = PyDict_New();
            _localDict = PyDict_New();
            
            _sqlparseModule = PyImport_ImportModule("sqlparse");
            if (_sqlparseModule == nullptr) {
                throw std::runtime_error("cannot import module 'sqlparse'");
            }
            
            PyMem_RawFree(programName);
            
            auto *parserCode = loadParserScript();
            PyEval_EvalCode(parserCode, _globalDict, _localDict);
        });
    }
    
    void PySQLParser::finalize() {
        if (Py_IsInitialized()) {
            Py_FinalizeEx();
        }
    }
    
    PyObject *PySQLParser::loadParserScript() {
        const std::string code =
#include "pysqlparser/test.py"
        ;
        
        return Py_CompileString(code.c_str(), "<pysqlparser internal>", Py_file_input);
    }
    
    PySQLParser::PySQLParser() {
    }
    
    std::string PySQLParser::getProcedureHint(const std::string &statement) {
        auto *pyGreetFn = PyDict_GetItemString(_localDict, "get_procedure_hint");
        assert(pyGreetFn != nullptr);
        
        auto *pyStatementValue = PyUnicode_FromStringAndSize(statement.c_str(), statement.size());
        auto *tuple = PyTuple_New(2);
        
        PyTuple_SetItem(tuple, 0, _sqlparseModule);
        PyTuple_SetItem(tuple, 1, pyStatementValue);
        
        auto *result = PyObject_Call(pyGreetFn, tuple, nullptr);
        
        if (Py_IsNone(result)) {
            std::cout << "WARN: no procedure hint!" << std::endl;
    
            return {};
        } else {
            Py_ssize_t resultSize = 0;
            auto *resultCStr = PyUnicode_AsUTF8AndSize(result, &resultSize);
            
            return std::string(resultCStr, resultSize);
        }
    }
    
    
}