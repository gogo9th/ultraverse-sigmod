#include <iostream>

#include "mariadb/state/new/StateLogReader.hpp"

#include "utils/log.hpp"

#include "Application.hpp"

using namespace ultraverse;
using namespace ultraverse::state;

class StateLogViewerApp: public ultraverse::Application {
public:
    StateLogViewerApp():
        Application(),
        _logger(createLogger("StateLogViewer"))
    {
    }
    
    std::string optString() override {
        return "i:s:e:vVh";
    }
    
    template <typename Iterator>
    std::string join(Iterator begin, Iterator end, std::string sep) {
        std::stringstream sstream;
        
        Iterator it = begin;
        
        if (it == end) {
            return "(empty)";
        }
        
        while (true) {
            sstream << *it;
            it++;
            
            if (it != end) {
                sstream << sep;
            } else {
                break;
            }
        }
        
        return sstream.str();
    }
    
    
    template <typename Iterator>
    std::string joinItemSet(Iterator begin, Iterator end, std::string sep) {
        std::stringstream sstream;
        
        Iterator it = begin;
        
        if (it == end) {
            return "";
        }
        
        while (true) {
            StateItem &item = *it;
            sstream << item.MakeRange()->MakeWhereQuery(item.name);
            it++;
            
            if (it != end) {
                sstream << sep;
            } else {
                break;
            }
        }
        
        return sstream.str();
    }
    
    int main() override {
        if (isArgSet('h') || !isArgSet('i')) {
            std::cout <<
            "state_log_viewer - state log viewer\n"
            "\n"
            "Options: \n"
            "    -i statelog    specify state log\n"
            "    -s startgid    \n"
            "    -e endgid      \n"
            "    -v             print additional info (prints itemset, whereset)\n"
            "    -V             print more additional info (prints beforehash, afterhash)\n"
            "    -h             print this help and exit application\n";

            return 0;
        }
        
        bool isVerbose = isArgSet('v');
        bool moreVerbose = isArgSet('V');
        
        gid_t startGid = isArgSet('s') ? std::stoul(getArg('s')) : 0;
        gid_t endGid = isArgSet('e') ? std::stoul(getArg('e')) : UINT32_MAX;
        
        
        v2::StateLogReader reader(".", getArg('i'));
        reader.open();
        
        while (reader.next()) {
            auto transactionHeader = reader.txnHeader();
            
            if (transactionHeader == nullptr || transactionHeader->gid > endGid) {
                break;
            } else if (transactionHeader->gid < startGid) {
                continue;
            }
            
            auto transaction = reader.txnBody();
            
            _logger->info("Transaction #{}", transaction->gid());
            _logger->info("    - ReadSet: {}",
                          join(transaction->readSet().begin(), transaction->readSet().end(), ", "));
            _logger->info("    - WriteSet: {}",
                          join(transaction->writeSet().begin(), transaction->writeSet().end(), ", "));
            
            
            _logger->info("    - Flags: {}", transaction->flags());
            
            _logger->info("    - Queries:");
            
            int i = 0;
            for (auto &query: transaction->queries()) {
                _logger->info("        [#{}] {}", i++, query->statement());
                _logger->info("            - Type: {}", query->type());
                _logger->info("            - Database: {}", query->database());
                _logger->info("            - Timestamp: {}", query->timestamp());
                _logger->info("            - AffectedRows: {}", query->affectedRows());
                _logger->info("            - ReadSet: {}",
                              join(query->readSet().begin(), query->readSet().end(), ", "));
                _logger->info("            - WriteSet: {}",
                              join(query->writeSet().begin(), query->writeSet().end(), ", "));
                _logger->info("            - ForeignKeySet: {}",
                              join(query->foreignKeySet().begin(), query->foreignKeySet().end(), ", "));
                
                if (isVerbose) {
                    _logger->info("            - ItemSet: {}",
                                  joinItemSet(query->itemSet().begin(), query->itemSet().end(), ", "));
        
                    _logger->info("            - WhereSet: {}",
                                  joinItemSet(query->whereSet().begin(), query->whereSet().end(), ", "));
                }
                
                _logger->info("            - Flags: {}", query->flags());
                
                
                if (moreVerbose) {
                    _logger->info("            - BeforeHash:");
                    for (const auto &pair: query->beforeHash()) {
                        _logger->info("                {}:\t{}", pair.first, pair.second.stringify());
                    }
                    _logger->info("            - AfterHash:");
                    for (const auto &pair: query->afterHash()) {
                        _logger->info("                {}:\t{}", pair.first, pair.second.stringify());
                    }
                }

                
                _logger->info("");
            }
            
            _logger->info("");
        }
    
        return 0;
    }
    
private:
    LoggerPtr _logger;
};

int main(int argc, char **argv) {
    StateLogViewerApp application;
    return application.exec(argc, argv);
}
