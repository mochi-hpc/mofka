/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DRIVER_HPP
#define MOFKA_DRIVER_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/Validator.hpp>
#include <diaspora/PartitionSelector.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Driver.hpp>

#include <yokan/cxx/database.hpp>
#include <yokan/cxx/collection.hpp>
#include <yokan/cxx/client.hpp>
#include <bedrock/DependencyMap.hpp>
#include <bedrock/ServiceGroupHandle.hpp>

#include <thallium.hpp>
#include <memory>
#include <unordered_set>

#include <mofka/MofkaPartitionInfo.hpp>

namespace mofka {

class MofkaClient;

/**
 * @brief A MofkaDriver object is a handle for a Mofka service
 * deployed on a set of servers.
 */
class MofkaDriver : public diaspora::DriverInterface,
                    public std::enable_shared_from_this<MofkaDriver> {

    friend class MofkaClient;

    thallium::engine            m_engine;
    bedrock::ServiceGroupHandle m_bsgh;
    yokan::Client               m_yk_client;
    yokan::Database             m_yk_master_db;
    std::string                 m_yk_master_info;

    public:

    MofkaDriver(
          thallium::engine engine,
          bedrock::ServiceGroupHandle bsgh,
          const std::pair<std::string, uint16_t>& masterDbInfo)
    : m_engine(std::move(engine))
    , m_bsgh(std::move(bsgh))
    , m_yk_client{m_engine.get_margo_instance()}
    , m_yk_master_db{
        m_yk_client.makeDatabaseHandle(
            m_engine.lookup(masterDbInfo.first).get_addr(),
                masterDbInfo.second)}
    , m_yk_master_info{"yokan:" + std::to_string(masterDbInfo.second) + "@" + masterDbInfo.first}
    {}

    /**
     * @brief Factory function.
     *
     * @param options Options.
     *
     * @return a new Driver.
     */
    static std::shared_ptr<DriverInterface> create(const diaspora::Metadata& options);

    /**
     * @brief Start a progress thread.
     */
    void startProgressThread() const;

    /**
     * @brief Get the internal Thallium engine.
     */
    thallium::engine engine() const {
        return m_engine;
    }

    /**
     * @brief Returns the number of servers this service currently has.
     */
    size_t numServers() const {
        return m_bsgh.size();
    }

    /**
     * @brief Create a topic with a given name, if it does not exist yet.
     *
     * @param name Name of the topic.
     * @param options Mofka-specific options.
     * @param validator Validator object to validate events pushed to the topic.
     * @param selector PartitionSelector object of the topic.
     * @param serializer Serializer to use for all the events in the topic.
     */
    void createTopic(std::string_view name,
                     const diaspora::Metadata& options,
                     std::shared_ptr<diaspora::ValidatorInterface> validator,
                     std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                     std::shared_ptr<diaspora::SerializerInterface> serializer) override;

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    std::shared_ptr<diaspora::TopicHandleInterface> openTopic(std::string_view name) const override;

    /**
     * @brief List the available topics.
     *
     * @return a mapping from topic name to their information.
     */
    std::unordered_map<std::string, diaspora::Metadata> listTopics() const override;

    /**
     * @brief Checks if a topic exists.
     */
    bool topicExists(std::string_view name) const override;

    /**
     * @brief Get the default ThreadPool.
     */
    std::shared_ptr<diaspora::ThreadPoolInterface> defaultThreadPool() const override;

    /**
     * @brief Create a ThreadPool.
     *
     * @param count Number of threads.
     *
     * @return a ThreadPool with the specified number of threads.
     */
    std::shared_ptr<diaspora::ThreadPoolInterface> makeThreadPool(diaspora::ThreadCount count) const override;

    typedef bedrock::DependencyMap Dependencies;

    /**
     * @brief Create a Yokan provider on the target server, returning the
     * name an address of the provider in a form that can be passed to
     * addDefaultPartition.
     *
     * If the configuration is not provided, an in-memory map database
     * will be used. Otherwise, the config parameter should be a Metadata containing
     * a JSON object acceptable by Yokan, e.g.:
     *
     * ```
     * {
     *    "database": {
     *        "type": "leveldb",
     *        "config": {
     *            "path": "/tmp/mofka/my-db",
     *            "create_if_missing": true
     *        }
     *    }
     * }
     * ```
     *
     * @param server_rank Rank of the server in which to add the provider.
     * @param config Configuration.
     * @param dependencies Dependencies, if required by the configuration.
     *
     * @return provider address in the form <provider-name>@<provider-address>.
     */
    std::string addDefaultMetadataProvider(
        size_t server_rank,
        const diaspora::Metadata& config = diaspora::Metadata{R"({"database":{"type":"map","config":{}}})"},
        const Dependencies& dependencies = Dependencies{});

    /**
     * @brief Create a Warabi provider on the target server, returning the
     * name an address of the provider in a form that can be passed to
     * addDefaultPartition.
     *
     * If the configuration is not provided, an in-memory target
     * will be used. Otherwise, the config parameter should be a Metadata containing
     * a JSON object acceptable by Yokan, e.g.:
     *
     * ```
     * {
     *    "target": {
     *        "type": "pmdk",
     *        "config": {
     *            "path": "/tmp/mofka/my-target",
     *            "create_if_missing_with_size": 10485760,
     *            "override_if_exists": true
     *        }
     *    }
     * }
     * ```
     *
     * @param server_rank Rank of the server in which to add the provider.
     * @param config Configuration.
     * @param dependencies Dependencies, if required by the configuration.
     *
     * @return provider address in the form <provider-name>@<provider-address>.
     */
    std::string addDefaultDataProvider(
        size_t server_rank,
        const diaspora::Metadata& config = diaspora::Metadata{R"({"target":{"type":"memory","config":{}}})"},
        const Dependencies& dependencies = Dependencies{});

    /**
     * @brief Create a new partition at the given server rank and add it to the topic.
     *
     * @param topic_name Name of the topic.
     * @param server_rank Rank of the server.
     * @param partition_type Type of partition.
     * @param partition_config Configuration for the partition.
     * @param dependencies Map of dependencies expected by the partition.
     * @param pool_name Pool name in the server.
     */
    void addCustomPartition(std::string_view topic_name,
                            size_t server_rank,
                            std::string_view partition_type = "memory",
                            const diaspora::Metadata& partition_config = diaspora::Metadata{"{}"},
                            const Dependencies& dependencies = {},
                            std::string_view pool_name = "");

    /**
     * @brief Add an in-memory partition. Full in-memory partitions are useful
     * for testing, but in general we advise using addDefaultPartition to add
     * a partition that is backed up by Yokan and Warabi providers for Metadata
     * and Data storage respectively.
     *
     * @param topic_name Name of the topic.
     * @param server_rank Rank of the server in which to add the partition.
     * @param pool_name Pool name in the server.
     */
    void addMemoryPartition(std::string_view topic_name,
                            size_t server_rank,
                            std::string_view pool_name = "");

    /**
     * @brief Add a partition backed by Mofka' default partition manager implementation.
     * This partition manager uses a Yokan provider for Metadata storage and a Warabi
     * provider for Data storage.
     *
     * @param topic_name Topic name.
     * @param server_rank Rank of the server in which to add the partition.
     * @param metadata_provider Locator of the metadata provider (e.g. my_yokan_provider@local)
     * @param data_provider Locator of the data provider (e.g. my_warabi_provider@some_address)
     * @param partition_config Configuration for the partition.
     * @param pool_name Pool name in the server.
     */
    void addDefaultPartition(std::string_view topic_name,
                             size_t server_rank,
                             std::string_view metadata_provider = {},
                             std::string_view data_provider = {},
                             const diaspora::Metadata& config = {},
                             std::string_view pool_name = "");

};

}

#endif
