//
// Created by cheesekun on 8/8/22.
//

#ifndef ULTRAVERSE_MARIADB_BINARYLOG_HPP
#define ULTRAVERSE_MARIADB_BINARYLOG_HPP

#include <memory>
#include <string>

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include "DBEvent.hpp"
#include "DBHandle.hpp"


namespace ultraverse::mariadb {
    
    class BinaryLog {
    public:
        BinaryLog(DBHandle &handle);
        
        void setFileName(std::string fileName);
        void setStartPosition(int startPosition);
        
        void open();
        void close();
        
        bool next();
        
        /**
         * @return
         */
        std::shared_ptr<base::DBEvent> currentEvent() const;
        MARIADB_RPL_EVENT *currentRawEvent() const;
    private:
        DBHandle &_handle;
        
        /**
         * TODO: how to free MARIADB_RPL *?
         */
    public:
        MARIADB_RPL *_rpl;
        MARIADB_RPL_EVENT *_event;
    };
}


#endif //ULTRAVERSE_MARIADB_BINARYLOG_HPP
