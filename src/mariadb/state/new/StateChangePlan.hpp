//
// Created by cheesekun on 10/27/22.
//

#ifndef ULTRAVERSE_STATECHANGEPLAN_HPP
#define ULTRAVERSE_STATECHANGEPLAN_HPP

#include <map>
#include <string>
#include <vector>

#include "Transaction.hpp"
#include "RangeComparisonMethod.hpp"

namespace ultraverse::state::v2 {

    class StateChangePlan {
    public:
        explicit StateChangePlan();
        
        const std::string &dbHost() const;
        void setDBHost(const std::string &dbHost);
    
        const std::string &dbUsername() const;
        void setDBUsername(const std::string &dbUsername);
        
        const std::string &dbPassword() const;
        void setDBPassword(const std::string &dbPassword);
    
        const std::string &dbName() const;
        void setDBName(const std::string &dbName);
        
        gid_t startGid() const;
        void setStartGid(gid_t startGid);
        
        gid_t endGid() const;
        void setEndGid(gid_t endGid);
        bool hasGidRange() const;

        gid_t replayFromGid() const;
        void setReplayFromGid(gid_t replayFromGid);
        bool hasReplayFromGid() const;
    
        std::vector<gid_t> &rollbackGids();
        std::map<gid_t, std::string> &userQueries();
        
        gid_t lowestGidAvailable() const;
        
        bool isRollbackGid(gid_t gid) const;
        bool hasUserQuery(gid_t gid) const;
        
        bool isDBDumpAvailable() const;
        const std::string &dbDumpPath() const;
        void setDBDumpPath(const std::string &dbdumpPath);
        
        const std::string &binlogPath() const;
        void setBinlogPath(const std::string &binlogPath);
        
        const std::string &stateLogPath() const;
        void setStateLogPath(const std::string &stateLogPath);
        
        const std::string &stateLogName() const;
        void setStateLogName(const std::string &stateLogName);

        const std::string &procCallLogPath() const;
        void setProcCallLogPath(const std::string &procCallLogPath);
        
        bool writeStateLog() const;
        void setWriteStateLog(bool writeStateLog);
        
        const std::string &reportPath() const;
        void setReportPath(const std::string &reportPath);

        const std::vector<std::string> &replaceQueries() const;
        void setReplaceQueries(std::vector<std::string> replaceQueries);

        bool isFullReplay() const;
        void setFullReplay(bool isFullReplay);
        
        bool isDryRun() const;
        void setDryRun(bool isDryRun);
        
        int threadNum() const;
        void setThreadNum(int threadNum);
        
        bool dropIntermediateDB() const;
        void setDropIntermediateDB(bool dropIntermediateDB);

        bool executeReplaceQuery() const;
        void setExecuteReplaceQuery(bool executeReplaceQuery);
        
        double autoRollbackRatio() const;
        void setAutoRollbackRatio(double autoRollbackRatio);

        bool performBenchInsert() const;
        void setPerformBenchInsert(bool performBenchInsert);
        
        RangeComparisonMethod rangeComparisonMethod() const;
        void setRangeComparisonMethod(RangeComparisonMethod rangeComparisonMethod);
        
        std::set<std::string> &keyColumns();
        std::vector<std::vector<std::string>> &keyColumnGroups();
        const std::vector<std::vector<std::string>> &keyColumnGroups() const;
        void setKeyColumnGroups(std::vector<std::vector<std::string>> keyColumnGroups);
        std::vector<std::pair<std::string, std::string>> &columnAliases();
        const std::vector<std::pair<std::string, std::string>> &columnAliases() const;
        
        std::vector<uint64_t> &skipGids();
        [[nodiscard]]
        const std::vector<uint64_t> &skipGids() const;
    
    private:
        std::string _dbHost;
        std::string _dbUsername;
        std::string _dbPassword;
        std::string _dbName;
        
        gid_t _startGid;
        gid_t _endGid;
        bool _hasGidRange;
        gid_t _replayFromGid;
        bool _hasReplayFromGid;
        
        std::vector<gid_t> _rollbackGids;
        std::map<gid_t, std::string> _userQueries;
        
        std::string _dbdumpPath;
        std::string _binlogPath;
        std::string _stateLogPath;
        std::string _stateLogName;
        std::string _procCallLogPath;

        bool _writeStateLog;
        std::string _reportPath;
        std::vector<std::string> _replaceQueries;
        
        std::set<std::string> _keyColumns;
        std::vector<std::vector<std::string>> _keyColumnGroups;
        std::vector<std::pair<std::string, std::string>> _columnAliases;
    
        std::vector<uint64_t> _skipGids;
        
        double _autoRollbackRatio;
        
        bool _isFullReplay;
        bool _isDryRun;
        bool _dropIntermediateDB;
        bool _executeReplaceQuery;

        bool _performBenchInsert;
        
        int _threadNum;
        
        RangeComparisonMethod _rangeComparisonMethod;
    };
    
}

#endif //ULTRAVERSE_STATECHANGEPLAN_HPP
