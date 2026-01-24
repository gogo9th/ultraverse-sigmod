//
// Created by cheesekun on 7/14/23.
//

#include "StateClusterWriter.hpp"

#include <stdexcept>

#include "ultraverse_state.pb.h"

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
        ultraverse::state::v2::proto::StateCluster protoCluster;
        cluster.toProtobuf(&protoCluster);
        if (!protoCluster.SerializeToOstream(&stream)) {
            throw std::runtime_error("failed to serialize state cluster protobuf");
        }

        stream.flush();
        stream.close();
    }
    
    void StateClusterWriter::writeTableDependencyGraph(TableDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ulttables";
        std::ofstream stream(fileName, std::ios::binary);
        ultraverse::state::v2::proto::TableDependencyGraph protoGraph;
        graph.toProtobuf(&protoGraph);
        if (!protoGraph.SerializeToOstream(&stream)) {
            throw std::runtime_error("failed to serialize table dependency graph protobuf");
        }

        stream.flush();
        stream.close();
    }
    
    void StateClusterWriter::readCluster(StateCluster &cluster) {
        std::string fileName = _logPath + "/" + _logName + ".ultcluster";
        std::ifstream stream(fileName, std::ios::binary);
        stream.seekg(0);

        ultraverse::state::v2::proto::StateCluster protoCluster;
        if (!protoCluster.ParseFromIstream(&stream)) {
            throw std::runtime_error("failed to read state cluster protobuf");
        }
        cluster.fromProtobuf(protoCluster);

        stream.close();
    }
    
    void StateClusterWriter::readTableDependencyGraph(TableDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ulttables";
        std::ifstream stream(fileName, std::ios::binary);

        ultraverse::state::v2::proto::TableDependencyGraph protoGraph;
        if (!protoGraph.ParseFromIstream(&stream)) {
            throw std::runtime_error("failed to read table dependency graph protobuf");
        }
        graph.fromProtobuf(protoGraph);

        stream.close();
    }
}
