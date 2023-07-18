//
// Created by cheesekun on 7/14/23.
//

#include <cereal/archives/binary.hpp>

#include "StateClusterWriter.hpp"

namespace ultraverse::state::v2 {
    StateClusterWriter::StateClusterWriter(const std::string &logPath, const std::string &logName):
        _logPath(logPath),
        _logName(logName)
    {
    
    }
    
    void StateClusterWriter::operator<<(StateCluster &cluster) {
        writeCluster(cluster);
    }
    
    void StateClusterWriter::operator<<(TableDependencyGraph &graph) {
        writeTableDependencyGraph(graph);
    }
    
    void StateClusterWriter::operator>>(StateCluster &cluster) {
        readCluster(cluster);
    }
    
    void StateClusterWriter::operator>>(TableDependencyGraph &graph) {
        readTableDependencyGraph(graph);
    }
    
    void StateClusterWriter::writeCluster(StateCluster &cluster) {
        std::string fileName = _logPath + "/" + _logName + ".ultcluster";
        std::ofstream stream(fileName, std::ios::binary);
        
        cereal::BinaryOutputArchive archive(stream);
        archive(cluster);
        
        stream.flush();
        stream.close();
    }
    
    void StateClusterWriter::writeTableDependencyGraph(TableDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ulttables";
        std::ofstream stream(fileName, std::ios::binary);
        
        cereal::BinaryOutputArchive archive(stream);
        archive(graph);
        
        stream.flush();
        stream.close();
    }
    
    void StateClusterWriter::readCluster(StateCluster &cluster) {
        std::string fileName = _logPath + "/" + _logName + ".ultcluster";
        std::ifstream stream(fileName, std::ios::binary);
        
        stream.seekg(0);
        
        cereal::BinaryInputArchive archive(stream);
        archive(cluster);
        
        stream.close();
    }
    
    void StateClusterWriter::readTableDependencyGraph(TableDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ulttables";
        std::ifstream stream(fileName, std::ios::binary);
        
        cereal::BinaryInputArchive archive(stream);
        archive(graph);
        
        stream.close();
    }
}
