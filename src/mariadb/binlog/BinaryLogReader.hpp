//
// Created by cheesekun on 2/1/23.
//

#ifndef ULTRAVERSE_BINARYLOGREADER_HPP
#define ULTRAVERSE_BINARYLOGREADER_HPP

#include <cstdint>

#include <iostream>
#include <fstream>
#include <string>
#include <memory>

#include "base/DBEvent.hpp"
#include "BinaryLogEvents.hpp"

namespace ultraverse::mariadb {
    class BinaryLogReaderBase {
    public:
        BinaryLogReaderBase(const std::string &filename);
        virtual ~BinaryLogReaderBase() = default;
        
        virtual void open() = 0;
        virtual void close() = 0;
    
    
        virtual bool seek(int64_t position) = 0;
        virtual bool next() = 0;
    
        virtual int pos() = 0;
    
        virtual std::shared_ptr<base::DBEvent> currentEvent() = 0;
    };
}

#endif //ULTRAVERSE_BINARYLOGREADER_HPP
