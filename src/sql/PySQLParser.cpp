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
#include "pysqlparser/parser_main.py"
        ;
        
        return Py_CompileString(code.c_str(), "<pysqlparser internal>", Py_file_input);
    }
    
    PySQLParser::PySQLParser() {
    }
    

    bool PySQLParser::queryMatches(const std::string &statementA, const std::string &statementB) {
        auto *pyQueryMatchesFn = PyDict_GetItemString(_localDict, "query_matches");
        assert(pyQueryMatchesFn != nullptr);
        
        auto *args = PyTuple_New(3);
    
        PyTuple_SetItem(args, 0, _sqlparseModule);
        
        // FIXME: 💩: Python GC에게 모든 것을 맡기지 마!
        PyTuple_SetItem(args, 1, PyUnicode_FromStringAndSize(statementA.c_str(), statementA.size()));
        PyTuple_SetItem(args, 2, PyUnicode_FromStringAndSize(statementB.c_str(), statementB.size()));
        
        auto *result = PyObject_Call(pyQueryMatchesFn, args, nullptr);
        
        // TODO: args 정리해야 하는데 수동으로 정리해버리면 죽음 (GC랑 충돌나는듯)

        if (PyErr_Occurred()) {
            PyErr_Print();
        }
        assert(PyBool_Check(result));

        return PyObject_IsTrue(result);
    }
    
}