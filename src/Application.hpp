//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_APPLICATION_HPP
#define ULTRAVERSE_APPLICATION_HPP

#include <string>
#include <unordered_map>

namespace ultraverse {
    class Application {
    public:
        explicit Application();
    
        virtual std::string optString() = 0;
        
        int exec(int argc, char **argv);
        virtual int main() = 0;
        
        bool isArgSet(char flag);
        std::string getArg(char flag);
        
        bool isEnvSet(std::string envName);
        std::string getEnv(std::string envName);
        
    protected:
        int argc() const;
        char **argv() const;
        
    private:
        void parseArgs(int argc, char **argv);
        
        std::unordered_map<char, std::string> _args;
        int _argc;
        char **_argv;
    };
}

#endif //ULTRAVERSE_APPLICATION_HPP
