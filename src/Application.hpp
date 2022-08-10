//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_APPLICATION_HPP
#define ULTRAVERSE_APPLICATION_HPP

#include "mariadb/state/StateThreadPool.h"

namespace ultraverse {
    class Application {
    public:
        Application(int argc, char **argv);
    
        int exec();
    };
}

#endif //ULTRAVERSE_APPLICATION_HPP
