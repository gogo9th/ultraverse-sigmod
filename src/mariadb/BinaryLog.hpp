//
// Created by cheesekun on 8/8/22.
//

#ifndef ULTRAVERSE_MARIADB_BINARYLOG_HPP
#define ULTRAVERSE_MARIADB_BINARYLOG_HPP

#include <memory>
#include <string>

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include "DBHandle.hpp"

namespace ultraverse::mariadb {
    
    class BinaryLog {
    public:
        BinaryLog(std::shared_ptr<DBHandle> handle);
        
        void setFileName(std::string fileName);
        void setStartPosition(int startPosition);
        
        void open();
        void close();
        
        bool next();
        
        const MARIADB_RPL_EVENT *currentEvent() const;
    private:
        std::shared_ptr<DBHandle> _handle;
        
        /**
         * TODO: how to free MARIADB_RPL *?
         */
        MARIADB_RPL *_rpl;
        MARIADB_RPL_EVENT *_event;
    };
}


#endif //ULTRAVERSE_MARIADB_BINARYLOG_HPP
