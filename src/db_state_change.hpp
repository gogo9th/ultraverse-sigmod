//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DB_STATE_CHANGE_HPP
#define ULTRAVERSE_DB_STATE_CHANGE_HPP

#include "utils/log.hpp"
#include "Application.hpp"

#include "mariadb/state/new/StateChanger.hpp"

namespace ultraverse {
    using namespace ultraverse::state::v2;
    
    namespace ActionType {
        enum Value {
            MAKE_CLUSTER,
            ROLLBACK,
            AUTO_ROLLBACK,
            PREPEND,
            FULL_REPLAY,
            REPLAY
        };
    }
    
    class Action {
    public:
        virtual ActionType::Value type() = 0;
    };
    
    class MakeClusterAction: public Action {
    public:
        MakeClusterAction();
        ActionType::Value type() override;
    };
    
    class RollbackAction: public Action {
    public:
        RollbackAction(gid_t gid);
        ActionType::Value type() override;
        
        gid_t gid() const;
        
    private:
        gid_t _gid;
    };
    
    class AutoRollbackAction: public Action {
    public:
        AutoRollbackAction(double ratio);
        ActionType::Value type() override;
        
        double ratio() const;
        
    private:
        double _ratio;
    };
    
    class PrependAction: public Action {
    public:
        PrependAction(gid_t gid, std::string sqlFile);
        ActionType::Value type() override;
        
        gid_t gid() const;
        std::string sqlFile() const;
        
    private:
        gid_t _gid;
        std::string _sqlFile;
    };
    
    class FullReplayAction: public Action {
    public:
        FullReplayAction();
        ActionType::Value type() override;
    };
    
    class ReplayAction: public Action {
    public:
        ReplayAction();
        ActionType::Value type() override;
    };
    
    
    class DBStateChangeApp: public Application {
    public:
        DBStateChangeApp();
        
        std::string optString() override;
        
        int main() override;
        
        void preparePlan(std::vector<std::shared_ptr<Action>> &actions, StateChangePlan &changePlan);
        bool confirm(std::string message);
        
        std::vector<std::string> split(const std::string &inputStr, char character);
        
        std::vector<std::shared_ptr<Action>> parseActions(std::string expression);
        void describeActions(const std::vector<std::shared_ptr<Action>> &actions);
        
        std::vector<std::vector<std::string>> buildKeyColumnGroups(std::string expression);
        std::set<std::pair<std::string, std::string>> buildColumnAliasesList(std::string expression);
        std::vector<uint64_t> buildSkipGidList(std::string gidsStr);
        
    private:
        LoggerPtr _logger;
    };
}

int main(int argc, char **argv);

#endif //ULTRAVERSE_DB_STATE_CHANGE_HPP
