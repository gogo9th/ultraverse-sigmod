//
// Created by cheesekun on 1/20/26.
//

#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fmt/format.h>

#include "StateIO.hpp"
#include "StateClusterWriter.hpp"

#include "ultraverse_state.pb.h"

namespace {
    std::string resolveMysqlBinaryPath() {
        constexpr const char *kMysqlBinaryEnvVars[] = {
            "MYSQL_BIN_PATH",
            "MYSQL_BIN",
            "MYSQL_PATH"
        };

        for (const auto *envName : kMysqlBinaryEnvVars) {
            const char *envValue = std::getenv(envName);
            if (envValue == nullptr || envValue[0] == '\0') {
                continue;
            }

            std::string candidate(envValue);
            struct stat st {};
            if (::stat(candidate.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
                if (!candidate.empty() && candidate.back() != '/') {
                    candidate.push_back('/');
                }
                candidate.append("mysql");
            }
            return candidate;
        }

        return "/usr/bin/mysql";
    }
}

namespace ultraverse::state::v2 {
    MockedStateLogReader::MockedStateLogReader() {
        rebuildIndex();
    }

    MockedStateLogReader::MockedStateLogReader(std::vector<Entry> entries):
        _entries(std::move(entries))
    {
        rebuildIndex();
    }

    void MockedStateLogReader::open() {
        _cursor = 0;
        _currentHeader = nullptr;
        _currentBody = nullptr;
    }

    void MockedStateLogReader::close() {
    }

    void MockedStateLogReader::reset() {
        open();
    }

    uint64_t MockedStateLogReader::pos() {
        return static_cast<uint64_t>(_cursor);
    }

    void MockedStateLogReader::seek(uint64_t pos) {
        _cursor = static_cast<size_t>(pos);
        if (_cursor > _entries.size()) {
            _cursor = _entries.size();
        }

        _currentHeader = nullptr;
        _currentBody = nullptr;
    }

    bool MockedStateLogReader::nextHeader() {
        if (_cursor >= _entries.size()) {
            _currentHeader = nullptr;
            return false;
        }

        _currentHeader = _entries[_cursor].header;
        _currentBody = nullptr;
        return true;
    }

    bool MockedStateLogReader::nextTransaction() {
        if (_cursor >= _entries.size()) {
            _currentBody = nullptr;
            return false;
        }

        _currentBody = _entries[_cursor].body;
        _cursor++;
        return true;
    }

    void MockedStateLogReader::skipTransaction() {
        if (_cursor < _entries.size()) {
            _cursor++;
        }

        _currentHeader = nullptr;
        _currentBody = nullptr;
    }

    std::shared_ptr<TransactionHeader> MockedStateLogReader::txnHeader() {
        return _currentHeader;
    }

    std::shared_ptr<Transaction> MockedStateLogReader::txnBody() {
        return _currentBody;
    }

    bool MockedStateLogReader::seekGid(gid_t gid) {
        auto it = _gidToIndex.find(gid);
        if (it == _gidToIndex.end()) {
            return false;
        }

        _cursor = it->second;
        _currentHeader = nullptr;
        _currentBody = nullptr;
        return true;
    }

    void MockedStateLogReader::addTransaction(const std::shared_ptr<Transaction> &transaction,
                                              gid_t gid,
                                              uint64_t timestamp,
                                              int xid,
                                              bool isSuccessful,
                                              uint8_t flags) {
        auto header = std::make_shared<TransactionHeader>();
        header->timestamp = timestamp;
        header->gid = gid;
        header->xid = xid;
        header->isSuccessful = isSuccessful;
        header->flags = flags;
        header->nextPos = 0;

        if (transaction != nullptr) {
            transaction->setGid(gid);
            transaction->setTimestamp(timestamp);
            transaction->setXid(xid);
            transaction->setFlags(flags);
        }

        _entries.push_back(Entry{header, transaction});
        rebuildIndex();
    }

    void MockedStateLogReader::setEntries(std::vector<Entry> entries) {
        _entries = std::move(entries);
        rebuildIndex();
    }

    void MockedStateLogReader::rebuildIndex() {
        _gidToIndex.clear();
        for (size_t i = 0; i < _entries.size(); i++) {
            if (_entries[i].header != nullptr) {
                _gidToIndex[_entries[i].header->gid] = i;
            }
        }
    }

    FileStateClusterStore::FileStateClusterStore(const std::string &logPath, const std::string &logName):
        _logPath(logPath),
        _logName(logName)
    {
    }

    void FileStateClusterStore::load(StateCluster &cluster) {
        StateClusterWriter writer(_logPath, _logName);
        writer >> cluster;
    }

    void FileStateClusterStore::save(StateCluster &cluster) {
        StateClusterWriter writer(_logPath, _logName);
        writer << cluster;
    }

    MockedStateClusterStore::MockedStateClusterStore() = default;

    MockedStateClusterStore::MockedStateClusterStore(std::string data):
        _data(std::move(data))
    {
    }

    void MockedStateClusterStore::load(StateCluster &cluster) {
        if (_data.empty()) {
            throw std::runtime_error("mocked cluster store is empty");
        }

        ultraverse::state::v2::proto::StateCluster protoCluster;
        if (!protoCluster.ParseFromString(_data)) {
            throw std::runtime_error("failed to read mocked state cluster protobuf");
        }
        cluster.fromProtobuf(protoCluster);
    }

    void MockedStateClusterStore::save(StateCluster &cluster) {
        ultraverse::state::v2::proto::StateCluster protoCluster;
        cluster.toProtobuf(&protoCluster);
        if (!protoCluster.SerializeToString(&_data)) {
            throw std::runtime_error("failed to serialize mocked state cluster protobuf");
        }
    }

    const std::string &MockedStateClusterStore::data() const {
        return _data;
    }

    void MockedStateClusterStore::setData(std::string data) {
        _data = std::move(data);
    }

    MySQLBackupLoader::MySQLBackupLoader(std::string host, std::string username, std::string password):
        _host(std::move(host)),
        _username(std::move(username)),
        _password(std::move(password))
    {
    }

    void MySQLBackupLoader::loadBackup(const std::string &dbName, const std::string &fileName) {
        int fd = ::open(fileName.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("failed to load backup file");
        }

        auto pid = fork();
        if (pid == -1) {
            throw std::runtime_error("failed to fork process");
        }

        if (pid == 0) {
            dup2(fd, STDIN_FILENO);
            std::string password = "-p" + _password;
            const std::string mysqlPath = resolveMysqlBinaryPath();
            int retval = execl(
                mysqlPath.c_str(),
                mysqlPath.c_str(),
                "-h", _host.c_str(),
                "-u", _username.c_str(),
                password.c_str(),
                dbName.c_str(),
                nullptr
            );

            close(fd);

            if (retval == -1) {
                throw std::runtime_error(fmt::format("failed to execute mysql: {}", strerror(errno)));
            }
        } else {
            close(fd);

            int wstatus = 0;
            waitpid(pid, &wstatus, 0);

            if (!WIFEXITED(wstatus) || WEXITSTATUS(wstatus) != 0) {
                throw std::runtime_error(
                    fmt::format("failed to restore backup: WEXITSTATUS {}", WEXITSTATUS(wstatus))
                );
            }
        }
    }
}
