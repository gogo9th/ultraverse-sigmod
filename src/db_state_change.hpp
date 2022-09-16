//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DB_STATE_CHANGE_HPP
#define ULTRAVERSE_DB_STATE_CHANGE_HPP

#include "utils/log.hpp"
#include "Application.hpp"

namespace ultraverse {
    class DBStateChangeApp: public Application {
    public:
        DBStateChangeApp();
        
        std::string optString() override;
        
        int main() override;
        
        bool confirm(std::string message);
        
        std::vector<std::string> buildKeyColumnList(std::string expression);
        
    private:
        LoggerPtr _logger;
        
    };
}

int main(int argc, char **argv);

#endif //ULTRAVERSE_DB_STATE_CHANGE_HPP
