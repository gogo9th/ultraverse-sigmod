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
    
    class DBStateChangeApp: public Application {
    public:
        DBStateChangeApp();
        
        std::string optString() override;
        
        int main() override;
        
        void preparePlan(const std::string &action, StateChangePlan &changePlan);
        bool confirm(std::string message);
        
        std::vector<std::string> buildKeyColumnList(std::string expression);
        std::set<std::pair<std::string, std::string>> buildColumnAliasesList(std::string expression);
        std::vector<uint64_t> buildSkipGidList(std::string gidsStr);
        
    private:
        LoggerPtr _logger;
    };
}

int main(int argc, char **argv);

#endif //ULTRAVERSE_DB_STATE_CHANGE_HPP
