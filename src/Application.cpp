#include <cstring>
#include <unistd.h>

#include "Application.hpp"

namespace ultraverse {
    Application::Application()
    {
    }

    void Application::parseArgs(int argc, char **argv) {
        auto optString = this->optString();
        char option;
        
        while ((option = getopt(argc, argv, optString.c_str())) != -1) {
            switch (option) {
                case '?':
                    break;
                default:
                    if (optarg == nullptr) {
                        _args[option] = "1";
                    } else {
                        auto length = strlen(optarg);
                        _args[option] = std::string(optarg, length);
                    }
            }
        }
    }
    
    bool Application::isArgSet(char flag) {
        auto it = _args.find(flag);
        return it != _args.end();
    }
    
    std::string Application::getArg(char flag) {
        return _args.at(flag);
    }
    
    bool Application::isEnvSet(std::string envName) {
        return getenv(envName.c_str()) == nullptr;
    }
    
    std::string Application::getEnv(std::string envName) {
        const char *envVar = getenv(envName.c_str());
        
        if (envVar == nullptr) {
            return std::string();
        }
        return std::string(envVar);
    }
    
    int Application::exec(int argc, char **argv) {
        _argc = argc;
        _argv = argv;
        
        parseArgs(argc, argv);
        return main();
    }
    
    int Application::argc() const {
        return _argc;
    }
    
    char **Application::argv() const {
        return _argv;
    }
    
}