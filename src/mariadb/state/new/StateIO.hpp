//
// Created by cheesekun on 1/20/26.
//

#ifndef ULTRAVERSE_STATE_IO_HPP
#define ULTRAVERSE_STATE_IO_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    class StateCluster;

    class IStateLogReader {
    public:
        virtual ~IStateLogReader() = default;

        virtual void open() = 0;
        virtual void close() = 0;
        virtual void reset() = 0;

        virtual uint64_t pos() = 0;
        virtual void seek(uint64_t pos) = 0;

        virtual bool nextHeader() = 0;
        virtual bool nextTransaction() = 0;

        virtual void skipTransaction() = 0;

        virtual std::shared_ptr<TransactionHeader> txnHeader() = 0;
        virtual std::shared_ptr<Transaction> txnBody() = 0;

        virtual bool seekGid(gid_t gid) = 0;
    };

    class IStateClusterStore {
    public:
        virtual ~IStateClusterStore() = default;
        virtual void load(StateCluster &cluster) = 0;
        virtual void save(StateCluster &cluster) = 0;
    };

    class IBackupLoader {
    public:
        virtual ~IBackupLoader() = default;
        virtual void loadBackup(const std::string &dbName, const std::string &fileName) = 0;
    };

    struct StateChangerIO {
        std::unique_ptr<IStateLogReader> stateLogReader;
        std::unique_ptr<IStateClusterStore> clusterStore;
        std::unique_ptr<IBackupLoader> backupLoader;
        bool closeStandardFds = true;
    };

    class MockedStateLogReader final: public IStateLogReader {
    public:
        struct Entry {
            std::shared_ptr<TransactionHeader> header;
            std::shared_ptr<Transaction> body;
        };

        MockedStateLogReader();
        explicit MockedStateLogReader(std::vector<Entry> entries);

        void open() override;
        void close() override;
        void reset() override;

        uint64_t pos() override;
        void seek(uint64_t pos) override;

        bool nextHeader() override;
        bool nextTransaction() override;

        void skipTransaction() override;

        std::shared_ptr<TransactionHeader> txnHeader() override;
        std::shared_ptr<Transaction> txnBody() override;

        bool seekGid(gid_t gid) override;

        void addTransaction(const std::shared_ptr<Transaction> &transaction,
                            gid_t gid,
                            uint64_t timestamp = 0,
                            int xid = 0,
                            bool isSuccessful = true,
                            uint8_t flags = 0);
        void setEntries(std::vector<Entry> entries);

    private:
        void rebuildIndex();

        std::vector<Entry> _entries;
        std::unordered_map<gid_t, size_t> _gidToIndex;
        size_t _cursor = 0;
        std::shared_ptr<TransactionHeader> _currentHeader;
        std::shared_ptr<Transaction> _currentBody;
    };

    class FileStateClusterStore final: public IStateClusterStore {
    public:
        FileStateClusterStore(const std::string &logPath, const std::string &logName);

        void load(StateCluster &cluster) override;
        void save(StateCluster &cluster) override;

    private:
        std::string _logPath;
        std::string _logName;
    };

    class MockedStateClusterStore final: public IStateClusterStore {
    public:
        MockedStateClusterStore();
        explicit MockedStateClusterStore(std::string data);

        void load(StateCluster &cluster) override;
        void save(StateCluster &cluster) override;

        const std::string &data() const;
        void setData(std::string data);

    private:
        std::string _data;
    };

    class MySQLBackupLoader final: public IBackupLoader {
    public:
        MySQLBackupLoader(std::string host, std::string username, std::string password);

        void loadBackup(const std::string &dbName, const std::string &fileName) override;

    private:
        std::string _host;
        std::string _username;
        std::string _password;
    };
}

#endif //ULTRAVERSE_STATE_IO_HPP
