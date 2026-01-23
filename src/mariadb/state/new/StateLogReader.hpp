//
// Created by cheesekun on 8/21/22.
//

#ifndef ULTRAVERSE_STATE_STATELOGREADER_HPP
#define ULTRAVERSE_STATE_STATELOGREADER_HPP

#include <fstream>
#include <memory>

#include "StateIO.hpp"
#include "Transaction.hpp"

#include "ColumnDependencyGraph.hpp"
#include "TableDependencyGraph.hpp"
#include "cluster/RowCluster.hpp"

namespace ultraverse::state::v2 {
    class GIDIndexReader;

    class StateLogReader: public IStateLogReader {
    public:
        StateLogReader(const std::string &logPath, const std::string &logName);
        ~StateLogReader();
        
        void open() override;
        void close() override;
        
        void reset() override;
        
        uint64_t pos() override;
        void seek(uint64_t pos) override;
        
        bool nextHeader() override;
        bool nextTransaction() override;
        
        void skipTransaction() override;
        
        bool next();
        
        std::shared_ptr<TransactionHeader> txnHeader() override;
        std::shared_ptr<Transaction> txnBody() override;

        bool seekGid(gid_t gid) override;
    
        void operator>>(RowCluster &rowCluster);
        void operator>>(ColumnDependencyGraph &graph);
        void operator>>(TableDependencyGraph &graph);
        
        void loadRowCluster(RowCluster &rowCluster);
        void loadColumnDependencyGraph(ColumnDependencyGraph &graph);
        void loadTableDependencyGraph(TableDependencyGraph &graph);
    private:
        std::string _logPath;
        std::string _logName;
        
        std::ifstream _stream;
        
        std::shared_ptr<TransactionHeader> _currentHeader;
        std::shared_ptr<Transaction> _currentBody;

        std::unique_ptr<GIDIndexReader> _gidIndexReader;
    };
}



#endif //ULTRAVERSE_STATE_STATELOGREADER_HPP
