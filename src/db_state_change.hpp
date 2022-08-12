//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DB_STATE_CHANGE_HPP
#define ULTRAVERSE_DB_STATE_CHANGE_HPP

#include "Application.hpp"

namespace ultraverse {
    class DBStateChangeApp : Application {
    public:
        DBStateChangeApp(int argc, char **argv): Application(argc, argv) {
        
        }
        
        int exec();
    };
}

int main(int argc, char **argv);

#endif //ULTRAVERSE_DB_STATE_CHANGE_HPP
