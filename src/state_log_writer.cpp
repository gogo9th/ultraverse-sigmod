#include <iostream>

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include "mariadb/DBHandle.hpp"
#include "mariadb/BinaryLog.hpp"

#include "mariadb/state/StateThreadPool.h"

#include "utils/log.hpp"
#include "Application.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

class StateLogWriter: public ultraverse::Application {
public:
    StateLogWriter(int argc, char **argv):
        Application(argc, argv),
        
        _logger(createLogger("StateLogWriter"))
    {
    
    }
    
    int exec() {
        spdlog::set_level(spdlog::level::debug);
        
        _logger->info("StateLogWriter (mariadb) started");
        _logger->info("establishing connection");
        
        DBHandle dbHandle;
        dbHandle.connect("localhost", 3306, "root", "mypass");
        
        auto binaryLog = std::make_shared<BinaryLog>(dbHandle);
        

        
        int pos = 4;
    
        binaryLog->setStartPosition(pos);
        binaryLog->open();
    
        while (true) {
            using namespace std::chrono_literals;
            _logger->info("FIXME: reading binary log from beginning");
            
            int i = 0;
            while (binaryLog->next()) {
                MARIADB_RPL_EVENT *event = binaryLog->currentRawEvent();
                
                i++;
            }
            
            _logger->info("{} event(s) processed. %d", i);
            _logger->debug("reached end");
            std::this_thread::sleep_for(5s);
        }
    
        binaryLog->close();
    
    
        return 0;
    }
    
private:
    LoggerPtr _logger;
};

int main(int argc, char **argv) {
    StateLogWriter application(argc, argv);
    return application.exec();
}
