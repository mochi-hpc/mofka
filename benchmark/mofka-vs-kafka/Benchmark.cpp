#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>
#include <memory>
#include <random>
#include <chrono>
#include <optional>
#include <mofka/MofkaDriver.hpp>
#include <mofka/KafkaDriver.hpp>
#include <mofka/BatchSize.hpp>
#include <mofka/Ordering.hpp>
#include <mofka/ThreadPool.hpp>
#include <mofka/Consumer.hpp>
#include <mofka/TopicHandle.hpp>
#include <librdkafka/rdkafka.h>
#include <unistd.h>
#include <mpi.h>

/**
 * @brief Computes the mapping between processes and partitions.
 */
static std::vector<size_t> computeMyPartitions(
        unsigned num_processes,
        unsigned my_process_rank,
        unsigned num_partitions) {
    if(num_processes < num_partitions) {
        // each process has multiple partitions
        std::vector<size_t> myPartitions;
        for(size_t i = my_process_rank; i < num_partitions; i += num_processes)
            myPartitions.push_back(i);
        return myPartitions;
    } else {
        // each partition is used by multiple processes
        return {my_process_rank % num_partitions};
    }
}

/**
 * @brief Generate a string of random letters of size n.
 */
static std::string random_bytes(size_t n) {
    const char charset[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::default_random_engine rng(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);
    std::string result;
    result.resize(n);
    for (size_t i = 0; i < n; ++i) {
        result[i] = charset[dist(rng)];
    }
    return result;
}

/**
 * @brief Create a topic in a Kafka cluster using librdkafka.
 *
 * @param bootstrap_servers Comma-separated list of bootstrap server addresses.
 * @param topic_name Name of the topic to create.
 * @param num_partitions Number of partitions.
 * @param replication_factor Replication factor.
 */
static void rdkafka_create_topic(
        const std::string &bootstrap_servers,
        const std::string &topic_name,
        int num_partitions,
        int replication_factor) {
    char errstr[512];
    auto conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), nullptr, 0);

    rd_kafka_t *rk = rd_kafka_new(
            RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error{"Could not create rd_kafka_t instance: " + std::string{errstr}};
    }
    auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

    // Create the NewTopic object
    auto new_topic = rd_kafka_NewTopic_new(
            topic_name.data(), num_partitions, replication_factor, errstr, sizeof(errstr));
    if (!new_topic) throw std::runtime_error{"Failed to create NewTopic object: " + std::string{errstr}};
    auto _new_topic = std::shared_ptr<rd_kafka_NewTopic_s>{new_topic, rd_kafka_NewTopic_destroy};

    // Create an admin options object
    auto options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
    if (!options) throw std::runtime_error{"Failed to create rd_kafka_AdminOptions_t"};
    auto _options = std::shared_ptr<rd_kafka_AdminOptions_s>{options, rd_kafka_AdminOptions_destroy};

    // Create a queue for the result of the operation
    auto queue = rd_kafka_queue_new(rk);
    auto _queue = std::shared_ptr<rd_kafka_queue_t>{queue, rd_kafka_queue_destroy};

    rd_kafka_NewTopic_t *new_topics[] = {new_topic};
    rd_kafka_CreateTopics(rk, new_topics, 1, options, queue);

    auto event = rd_kafka_queue_poll(queue, 10000);
    if (!event) throw std::runtime_error{"Timed out waiting for CreateTopics result"};
    auto _event = std::shared_ptr<rd_kafka_event_t>{event, rd_kafka_event_destroy};

    // Check if the event type is CreateTopics result
    if (rd_kafka_event_type(event) != RD_KAFKA_EVENT_CREATETOPICS_RESULT)
        throw std::runtime_error{"Unexpected event type when waiting for CreateTopics"};

    // Extract the result from the event
    auto result = rd_kafka_event_CreateTopics_result(event);
    size_t topic_count;
    auto topics_result = rd_kafka_CreateTopics_result_topics(result, &topic_count);

    // Check the results for errors
    for (size_t i = 0; i < topic_count; i++) {
        const rd_kafka_topic_result_t *topic_result = topics_result[i];
        if (rd_kafka_topic_result_error(topic_result) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw std::runtime_error{"Failed to create topic: "
                + std::string{rd_kafka_err2str(rd_kafka_topic_result_error(topic_result))}};
        }
    }

    sleep(5); // for some reasons Kafka is really lazy
    spdlog::info("Topic {} created successfully!", topic_name);
}

/**
 * @brief Produce messages into a Kafka topic using librdkafka.
 *
 * @param bootstrap_servers Comma-separated list of bootstrap server addresses.
 * @param topic_name Name of the topic in which to produce events.
 * @param num_partitions Number of partitions in the topic.
 * @param num_events Number of events.
 * @param message_size Size of each message.
 * @param warmup_events Number of warmup events to produce before actually measuring performance.
 * @param flush_every Frequency of flush calls.
 */
static void rdkafka_produce_messages(
        const std::string &bootstrap_servers,
        const std::string &topic_name,
        int num_partitions,
        int num_events,
        int message_size,
        int warmup_events,
        int flush_every) {

    int rank, num_producers;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_producers);
    auto my_partitions = computeMyPartitions(num_producers, rank, num_partitions);

    MPI_Barrier(MPI_COMM_WORLD);

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), nullptr, 0);
    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, nullptr, 0);

    int total_num_events = warmup_events + num_events;
    spdlog::info("Preparing {} messages...", total_num_events);
    std::vector<std::string> messages(total_num_events);
    for(auto& msg : messages)
        msg = random_bytes(message_size);

    decltype(std::chrono::high_resolution_clock::now()) t_start;
    spdlog::info("Producing {} messages...", total_num_events);

    double flush_time = 0.0;

    MPI_Barrier(MPI_COMM_WORLD);
    t_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < total_num_events; ++i) {
        if (i == warmup_events) {
            while(RD_KAFKA_RESP_ERR__TIMED_OUT == rd_kafka_flush(producer, 100)) {};
            MPI_Barrier(MPI_COMM_WORLD);
            t_start = std::chrono::high_resolution_clock::now();
        }
        auto partition_index = my_partitions[i % my_partitions.size()];
        auto& event = messages[i];
        rd_kafka_producev(producer,
                          RD_KAFKA_V_TOPIC(topic_name.c_str()),
                          RD_KAFKA_V_PARTITION(partition_index),
                          RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                          RD_KAFKA_V_VALUE(const_cast<char*>(event.data()), event.size()),
                          RD_KAFKA_V_END);

        rd_kafka_poll(producer, 0);

        if (flush_every > 0 && (i + 1) % flush_every == 0) {
            auto t_flush_start = std::chrono::high_resolution_clock::now();
            while(RD_KAFKA_RESP_ERR__TIMED_OUT == rd_kafka_flush(producer, 100)) {};
            auto t_flush_end = std::chrono::high_resolution_clock::now();
            if(i >= warmup_events) {
                flush_time += std::chrono::duration<double>(t_flush_end - t_flush_start).count();
            }
        }
    }
    auto t_flush_start = std::chrono::high_resolution_clock::now();
    while(RD_KAFKA_RESP_ERR__TIMED_OUT == rd_kafka_flush(producer, 100)) {};
    auto t_flush_end = std::chrono::high_resolution_clock::now();
    flush_time += std::chrono::duration<double>(t_flush_end - t_flush_start).count();

    MPI_Barrier(MPI_COMM_WORLD);

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    spdlog::info("Successfully produced {} messages in {} seconds (including {} seconds flush time)",
                 num_events, elapsed, flush_time);

    spdlog::info("AGGREGATE PRODUCTION RATE:\t {0:.2f} events/sec,\t {1:.2f} MB/sec",
                 ((size_t)num_producers)*((size_t)num_events)/elapsed,
                 ((size_t)num_producers)*((size_t)num_events)*message_size/(elapsed*1024*1024));

    rd_kafka_destroy(producer);
}

/**
 * @brief Consume messages from a Kafka cluster using librdkafka.
 *
 * @param bootstrap_servers Comma-separated list of bootstrap server addresses.
 * @param consumer_name Name of the consumer.
 * @param topic_name Topic name.
 * @param num_events Number of events to consumer.
 * @param acknowledge_every  Frequency of commits.
 * @param warmup_events Number of events to consume before starting to measure time.
 */
static void rdkafka_consume_messages(
        const std::string &bootstrap_servers,
        const std::string &consumer_name,
        const std::string &topic_name,
        mofka::BatchSize batch_size,
        int num_events,
        std::optional<int> acknowledge_every,
        int warmup_events) {

    auto batch = batch_size.value;
    if(batch == 0 || batch == mofka::BatchSize::Adaptive().value)
        batch = 1024;

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "socket.timeout.ms", "100000", nullptr, 0);
    rd_kafka_conf_set(conf, "socket.keepalive.enable", "true", nullptr, 0);
    rd_kafka_conf_set(conf, "api.version.request", "false", nullptr, 0);
    rd_kafka_conf_set(conf, "api.version.fallback.ms", "0", nullptr, 0);
    rd_kafka_conf_set(conf, "api.version.request.timeout.ms", "200", nullptr, 0);
    rd_kafka_conf_set(conf, "reconnect.backoff.ms", "0", nullptr, 0);
    rd_kafka_conf_set(conf, "reconnect.backoff.max.ms", "0", nullptr, 0);

    // figure out the number of partitions
    int num_partitions = 0;
    {
        rd_kafka_t* rk;
        const rd_kafka_metadata_t* metadata;
        char errstr[512];

        auto tmp_conf = rd_kafka_conf_dup(conf);
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, tmp_conf, errstr, sizeof(errstr));
        if (!rk) {
            rd_kafka_conf_destroy(tmp_conf);
            rd_kafka_conf_destroy(conf);
            throw mofka::Exception{std::string{"Error creating Kafka handle: "} + errstr};
        }
        auto _rk = std::shared_ptr<rd_kafka_t>{rk, rd_kafka_destroy};

        sleep(5);

        rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 1, NULL, &metadata, 10000);
        if (err) {
            rd_kafka_conf_destroy(conf);
            throw mofka::Exception{std::string{"Error fetching metadata: "} + rd_kafka_err2str(err)};
        }
        auto _metadata = std::shared_ptr<const rd_kafka_metadata_t>{metadata, rd_kafka_metadata_destroy};

        const rd_kafka_metadata_topic_t *topic_metadata = NULL;
        for (int i = 0; i < metadata->topic_cnt; i++) {
            if (strcmp(metadata->topics[i].topic, topic_name.data()) == 0) {
                topic_metadata = &metadata->topics[i];
                break;
            }
        }

        if (!topic_metadata) {
            rd_kafka_conf_destroy(conf);
            throw mofka::Exception{std::string{"Topic \""} + topic_name.data() + "\" does not exist"};
        }

        num_partitions = topic_metadata->partition_cnt;
    }
    spdlog::info("Found topic to have {} partitions", num_partitions);

    rd_kafka_conf_set(conf, "group.id", consumer_name.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", nullptr, 0);
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", nullptr, 0);

    int rank, num_consumers;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_consumers);
    auto my_partitions = computeMyPartitions(num_consumers, rank, num_partitions);

    //if(rank == 0) rd_kafka_conf_set(conf, "debug", "all", nullptr, 0);
    rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, nullptr, 0);
    sleep(2);

    rd_kafka_poll_set_consumer(consumer);
    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    for(auto i : my_partitions) {
        rd_kafka_topic_partition_list_add(topics, topic_name.c_str(), i);
    }
    rd_kafka_resp_err_t assign_err = rd_kafka_assign(consumer, topics);
    if(assign_err) {
        std::cout << "Process " << rank << ": rd_kafka_assign error: "
                  << rd_kafka_err2str(assign_err) << std::endl;
    }
    sleep(2);

    decltype(std::chrono::high_resolution_clock::now()) t_start;

    spdlog::info("Consuming {} messages (including {} warmup messages)...",
                 warmup_events + num_events, warmup_events);

    std::vector<std::string> messages;
    std::vector<rd_kafka_message_t*> raw_messages(batch);

    MPI_Barrier(MPI_COMM_WORLD);

    auto queue = rd_kafka_queue_get_consumer(consumer);

    int i = 0;
    double commit_time = 0.0;
    t_start = std::chrono::high_resolution_clock::now();
    size_t message_size = 0;

    while(true) {

        auto msg_received = rd_kafka_consume_batch_queue(
                queue, 100, raw_messages.data(), raw_messages.size());

        for(ssize_t j = 0; j < msg_received; j++) {
            auto msg = raw_messages[j];
            if (msg && !msg->err) {
                message_size = message_size == 0 ? msg->len : message_size;
                messages.emplace_back((const char*)msg->payload, msg->len);
                if (i >= warmup_events) {
                    if (acknowledge_every.has_value()
                    && (i - warmup_events + 1) % acknowledge_every.value() == 0) {
                        auto t_commit_start = std::chrono::high_resolution_clock::now();
                        rd_kafka_commit_message(consumer, msg, 0);
                        auto t_commit_end = std::chrono::high_resolution_clock::now();
                        if(i > warmup_events) {
                            commit_time += std::chrono::duration<double>(t_commit_end - t_commit_start).count();
                        }
                    }
                }
                i += 1;
            }
            if(msg && msg->err) {
                std::cout << rd_kafka_message_errstr(msg) << std::endl;
            }
            if(msg) {
                rd_kafka_message_destroy(msg);
                if (i % 100000 == 0 && rank == 0) {
                    auto elapsed = std::chrono::duration<double>(
                            std::chrono::high_resolution_clock::now() - t_start).count();
                    std::cout << "Process " << rank << " consumed "
                              << i << " messages in " << elapsed << std::endl;
                }
            }
            if (i == warmup_events + num_events) goto out;
        }
    }
out:

    MPI_Barrier(MPI_COMM_WORLD);
    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    spdlog::info("Successfully consumed {} messages in {} seconds (including {} seconds commit time)",
                 num_events + warmup_events, elapsed, commit_time);

    spdlog::info("AGGREGATE CONSUMPTION RATE:\t {0:.2f} events/sec,\t {1:.2f} MB/sec",
                 ((size_t)i)*((size_t)num_consumers)/elapsed,
                 ((size_t)i)*((size_t)num_consumers)*message_size/(elapsed*1024*1024));

    rd_kafka_queue_destroy(queue);
    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);
}

/**
 * @brief Create a topic in a Mofka service. The partitions will be
 * created in a round-robbin manner across its servers.
 *
 * @param driver MofkaDriver
 * @param topic_name Topic name
 * @param num_partitions Number of partitions
 */
static void createMofkaTopic(
    mofka::MofkaDriver driver, const std::string& topic_name, unsigned num_partitions,
    const std::string& partition_type,
    const std::string& database_type,
    const std::string& database_path_prefix,
    const std::string& storage_type,
    const std::string& storage_path_prefix,
    size_t storage_size) {
    driver.createTopic(topic_name);
    auto num_servers = driver.numServers();
    for(unsigned i = 0; i < num_partitions; ++i) {
        auto server_rank = i % num_servers;
        if(partition_type == "memory") {
            driver.addMemoryPartition(topic_name, server_rank);
        } else if(partition_type == "default") {
            // Create metadata provider
            auto metadata_config = mofka::Metadata{R"({
                "buffer_cache": {
                    "type": "lru",
                    "margin": 0.1,
                    "capacity": 32
                },
                "database":{
                    "type":"map",
                    "config":{}
                }
            })"};
            metadata_config.json()["database"]["type"] = database_type;
            if(database_type != "map" && database_type != "unordered_map") {
                metadata_config.json()["database"]["config"]["path"]
                    = database_path_prefix + "/" + topic_name + "_metadata_" + std::to_string(i);
                metadata_config.json()["database"]["config"]["create_if_missing"] = true;
            }

            auto metadata_provider = driver.addDefaultMetadataProvider(
                server_rank, metadata_config);
            // Create data provider
            auto data_config = mofka::Metadata{R"({"target":{"type":"memory","config":{}}})"};
            data_config.json()["target"]["type"] = storage_type;
            if(storage_type != "memory") {
                data_config.json()["target"]["config"]["path"]
                    = storage_path_prefix + "/" + topic_name + "_data_" + std::to_string(i);
                data_config.json()["target"]["config"]["override_if_exists"] = true;
            }
            if(storage_type == "pmdk") {
                data_config.json()["target"]["config"]["create_if_missing_with_size"] = storage_size;
            }
            auto data_provider = driver.addDefaultDataProvider(
                server_rank, metadata_config);
            // Create the partition
            driver.addDefaultPartition(
                topic_name, server_rank,
                metadata_provider, data_provider);
        } else {
            throw mofka::Exception{"Invalid partition type"};
        }
    }
}

/**
 * @brief Create topic in a Kafka service.
 *
 * @param driver KafkaDriver
 * @param topic_name Topic name
 * @param num_partitions Number of partitions
 */
static void createKafkaTopic(mofka::KafkaDriver driver,
                             const std::string& topic_name,
                             unsigned num_partitions) {
    driver.createTopic(topic_name, num_partitions);
    sleep(5);
}
/**
 * @brief Template produce function that can accept either a MofkaDriver or a KafkaDriver
 * as template parameter. It produces events in a specified topic.
 *
 * @tparam Driver Driver type (MofkaDriver or KafkaDriver).
 * @param driver Driver.
 * @param topic_name Topic name.
 * @param num_events Number of events to produce.
 * @param threads Number of backing threads for background work.
 * @param batch_size Batch size.
 * @param flush_every Frequency of flush operations.
 * @param metadata_size Size of the metadata.
 * @param data_size Size of the data part.
 * @param warmup_events Number of warmup events before measuring performance.
 * @param engine Optional thallium engine (used for MofkaDriver).
 */
template <typename Driver>
static void produce(Driver driver, const std::string& topic_name, int num_events,
                    int threads, mofka::BatchSize batch_size, int flush_every,
                    int metadata_size, int data_size, int warmup_events,
                    std::optional<thallium::engine> engine = std::nullopt) {
    spdlog::info("Opening topic {}", topic_name);
    auto topic = driver.openTopic(topic_name);

    auto thread_pool = mofka::ThreadPool{mofka::ThreadCount{(size_t)threads}};
    auto ordering = mofka::Ordering::Strict;
    int total_num_events = warmup_events + num_events;

    spdlog::info("Preparing metadata and data for {} events", total_num_events);
    std::vector<std::string> data(total_num_events);
    std::vector<mofka::Metadata> metadata(total_num_events);
    for (int i = 0; i < total_num_events; ++i) {
        data[i] = random_bytes(data_size);
        metadata[i] = mofka::Metadata{random_bytes(metadata_size)};
    }

    int rank, num_producers;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_producers);
    auto num_partitions = topic.partitions().size();
    auto my_partitions = computeMyPartitions(num_producers, rank, num_partitions);

    spdlog::info("Creating producer");
    auto producer = topic.producer(batch_size, thread_pool, ordering);

    MPI_Barrier(MPI_COMM_WORLD);

    if (warmup_events > 0) {
        spdlog::info("Start producing {} for warmup...", warmup_events);
        for (int i = 0; i < warmup_events; ++i) {
            auto partition_index = my_partitions[i % my_partitions.size()];
            producer.push(metadata[i],
                          mofka::Data{data[i].data(), data[i].size()},
                          partition_index);
        }
        producer.flush();
    }
    if(engine) {
        /* Reset all internal RPC statistics. Use the following environmane variables
         * to enable RPC statistics for the MofkaDriver:
         *
         * export MARGO_ENABLE_MONITORING=1
         * export MARGO_MONITORING_FILENAME_PREFIX=mofka
         * export MARGO_MONITORING_DISABLE_TIME_SERIES=true
         */
        margo_monitor_dump(engine.value().get_margo_instance(), nullptr, nullptr, true);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    spdlog::info("Start producing {} events...", num_events);
    auto t_start = std::chrono::high_resolution_clock::now();
    double t_flush = 0;
    if(!my_partitions.empty()) {
        for (int i = 0; i < num_events; ++i) {
            int j = i + warmup_events;
            auto partition_index = my_partitions[j % my_partitions.size()];
            producer.push(metadata[j],
                          mofka::Data{data[j].data(), data[j].size()},
                          partition_index);
            if (flush_every && (j + 1) % flush_every == 0) {
                auto t_flush_start = std::chrono::high_resolution_clock::now();
                producer.flush();
                auto t_flush_end = std::chrono::high_resolution_clock::now();
                t_flush += std::chrono::duration<double>(t_flush_end - t_flush_start).count();
            }
        }
    }
    auto t_flush_start = std::chrono::high_resolution_clock::now();
    producer.flush();
    auto t_flush_end = std::chrono::high_resolution_clock::now();
    t_flush += std::chrono::duration<double>(t_flush_end - t_flush_start).count();
    MPI_Barrier(MPI_COMM_WORLD);
    auto t_end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(t_end - t_start).count();
    spdlog::info("Marking topic as complete");
    if(rank == 0)
        topic.markAsComplete();
    spdlog::info("Producing {} events took {} seconds (including {} seconds flush time)", num_events,
                 elapsed, t_flush);

    spdlog::info("AGGREGATE PRODUCTION RATE:\t {0:.2f} events/sec,\t {1:.2f} MB/sec",
                 ((size_t)num_events)*((size_t)num_producers)/elapsed,
                 ((size_t)num_events)*((size_t)num_producers)*(metadata_size + data_size)/(elapsed*1024*1024));

}

/**
 * @brief Main "produce" function.
 *
 * @param argc argc-1 from main.
 * @param argv argv+1 from main.
 */
static void produce(int argc, char** argv) {

    auto backend_name = std::string{argv[1]};
    if(backend_name != "mofka" && backend_name != "kafka" && backend_name != "rdkafka")
        throw mofka::Exception{"Backend name should be either \"mofka\", \"kafka\", or \"rdkafka\""};

    TCLAP::CmdLine cmd("MOFKA/Kafka CLI", ' ', "1.0");
    TCLAP::ValueArg<std::string> bootstrapArg("b", "bootstrap-file", "Bootstrap file or server",
                                              true, "", "string");
    TCLAP::ValueArg<std::string> topicArg("t", "topic-name", "Topic name", true, "", "string");
    TCLAP::ValueArg<unsigned> eventsArg("n", "num-events", "Number of events", true, 0, "int");
    TCLAP::ValueArg<unsigned> threadsArg("p", "threads", "Number of threads", false, 0, "int");
    TCLAP::ValueArg<unsigned> batchSizeArg("s", "batch-size", "Batch size", false, 0, "int");
    TCLAP::ValueArg<unsigned> flushEveryArg("f", "flush-every", "Flush every N events", false, 0, "int");
    TCLAP::ValueArg<unsigned> metadataSizeArg("m", "metadata-size", "Metadata size", false, 8, "int");
    TCLAP::ValueArg<unsigned> dataSizeArg("d", "data-size", "Data size", false, 16, "int");
    TCLAP::ValueArg<unsigned> warmupArg("w", "num-warmup-events", "Number of warmup events", false, 0, "int");
    TCLAP::ValueArg<unsigned> partitionsArgs("q", "num-partitions", "Number of partitions", false, 1, "int");

    // mofka provider
    TCLAP::ValuesConstraint<std::string> allowedPartitionTypes(
        {"memory", "default"});
    TCLAP::ValueArg<std::string> partitionsTypeArgs(
        "", "mofka-partition-type", "Type of partition", false, "memory", &allowedPartitionTypes);

    // yokan provider
    TCLAP::ValuesConstraint<std::string> allowedDatabaseTypes(
        {"map", "berkeleydb", "gdbm", "leveldb", "rocksdb", "null",
         "tkrzw", "unordered_map", "unqlite", "log", "array"});
    TCLAP::ValueArg<std::string> databaseTypeArgs(
        "", "mofka-database-type", "Type of database for Mofka to use", false, "map", &allowedDatabaseTypes);
    TCLAP::ValueArg<std::string> databasePathPrefixArgs(
        "", "mofka-database-path-prefix", "Path prefix for Mofka databases",
        false, "/tmp/mofka/databases", "path");

    // warabi provider
    TCLAP::ValuesConstraint<std::string> allowedStorageTypes(
        {"memory", "abtio", "pmdk"});
    TCLAP::ValueArg<std::string> storageTypeArgs(
        "", "mofka-storage-type", "Type of storage for Mofka to use", false, "memory", &allowedStorageTypes);
    TCLAP::ValueArg<std::string> storagePathPrefixArgs(
        "", "mofka-storage-path-prefix", "Path prefix for Mofka data storage",
        false, "/tmp/mofka/storage", "path");
    TCLAP::ValueArg<size_t> storageSizeArgs(
        "", "mofka-storage-size", "Storage size for Mofka targets (relevant only for pmdk targets)",
        false, (size_t)(1024*1024*1024), "path");

    cmd.add(bootstrapArg);
    cmd.add(topicArg);
    cmd.add(eventsArg);
    cmd.add(threadsArg);
    cmd.add(batchSizeArg);
    cmd.add(flushEveryArg);
    cmd.add(metadataSizeArg);
    cmd.add(dataSizeArg);
    cmd.add(warmupArg);
    cmd.add(partitionsArgs);

    cmd.add(partitionsTypeArgs);
    cmd.add(databaseTypeArgs);
    cmd.add(databasePathPrefixArgs);
    cmd.add(storageTypeArgs);
    cmd.add(storagePathPrefixArgs);
    cmd.add(storageSizeArgs);

    cmd.parse(argc - 1, argv + 1);

    auto bootstrap_file = bootstrapArg.getValue();
    mofka::BatchSize batch_size = batchSizeArg.isSet() ?
        mofka::BatchSize{(size_t)batchSizeArg.getValue()}
        : mofka::BatchSize::Adaptive();

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    spdlog::info("=======================================================");
    spdlog::info("Producer will be executed with the following parameters");
    spdlog::info("  backend: {}", backend_name);
    spdlog::info("  bootstrap: {}", bootstrap_file);
    spdlog::info("  topic: {}", topicArg.getValue());
    spdlog::info("  events: {}", eventsArg.getValue());
    spdlog::info("  threads: {}", threadsArg.getValue());
    spdlog::info("  batch size: {}", batch_size.value);
    spdlog::info("  flush every: {}", flushEveryArg.getValue());
    spdlog::info("  metadata size: {}", metadataSizeArg.getValue());
    spdlog::info("  data size: {}", dataSizeArg.getValue());
    spdlog::info("  warmup events: {}", warmupArg.getValue());
    spdlog::info("  partitions: {}", partitionsArgs.getValue());
    if(backend_name == "mofka") {
        spdlog::info("  mofka partition type: {}", partitionsTypeArgs.getValue());
        if(partitionsTypeArgs.getValue() != "memory") {
            spdlog::info("  mofka database type: {}", databaseTypeArgs.getValue());
            if(databaseTypeArgs.getValue().find("map") == std::string::npos) {
                spdlog::info("  mofka database path prefix: {}", databasePathPrefixArgs.getValue());
            }
            spdlog::info("  mofka storage type: {}", storageTypeArgs.getValue());
            if(storageTypeArgs.getValue() != "memory") {
                spdlog::info("  mofka storage path prefix: {} ", storageTypeArgs.getValue());
                if(storageTypeArgs.getValue() == "pmdk") {
                    spdlog::info("  mofka storage size: {}", storageSizeArgs.getValue());
                }
            }
        }
    }
    spdlog::info("=======================================================");

    if(backend_name == "mofka") {
        auto driver = mofka::MofkaDriver{bootstrap_file, true};
        margo_set_progress_when_needed(driver.engine().get_margo_instance(), true);
        if(rank == 0) {
            createMofkaTopic(
                driver, topicArg.getValue(), partitionsArgs.getValue(),
                partitionsTypeArgs.getValue(),
                databaseTypeArgs.getValue(), databasePathPrefixArgs.getValue(),
                storageTypeArgs.getValue(), storagePathPrefixArgs.getValue(),
                storageSizeArgs.getValue());
        }
        MPI_Barrier(MPI_COMM_WORLD);
        produce(driver,
                topicArg.getValue(),
                eventsArg.getValue(),
                threadsArg.getValue(),
                batch_size,
                flushEveryArg.getValue(),
                metadataSizeArg.getValue(),
                dataSizeArg.getValue(),
                warmupArg.getValue(),
                driver.engine());
    } else if(backend_name == "kafka") {
        auto driver = mofka::KafkaDriver{bootstrap_file};
        if(rank == 0) {
            createKafkaTopic(driver, topicArg.getValue(), partitionsArgs.getValue());
        }
        MPI_Barrier(MPI_COMM_WORLD);
        produce(driver,
                topicArg.getValue(),
                eventsArg.getValue(),
                threadsArg.getValue(),
                batch_size,
                flushEveryArg.getValue(),
                metadataSizeArg.getValue(),
                dataSizeArg.getValue(),
                warmupArg.getValue());
    } else { /* rdkafka */
        if(rank == 0) {
            rdkafka_create_topic(
                    bootstrapArg.getValue(),
                    topicArg.getValue(),
                    partitionsArgs.getValue(), 1);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        rdkafka_produce_messages(
            bootstrapArg.getValue(),
            topicArg.getValue(),
            partitionsArgs.getValue(),
            eventsArg.getValue(),
            dataSizeArg.getValue() + metadataSizeArg.getValue(),
            warmupArg.getValue(),
            flushEveryArg.getValue());
    }
}

template <typename Driver>
static void consume(Driver driver, const std::string& consumer_name, const std::string& topic_name,
                    int threads, mofka::BatchSize batch_size, double data_selection,
                    std::optional<int> acknowledge_every, int warmup_events, int num_events) {
    spdlog::info("Opening topic {}", topic_name);
    auto topic = driver.openTopic(topic_name);

    if (data_selection < 0 || data_selection > 1) {
        throw std::invalid_argument("data_selection should be in [0,1]");
    }

    int rank, num_consumers;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_consumers);
    auto num_partitions = topic.partitions().size();
    auto my_partitions = computeMyPartitions(num_consumers, rank, num_partitions);

#if 0
    auto thread_pool = mofka::ThreadPool{mofka::ThreadCount{(size_t)threads}};
#endif

    mofka::DataSelector data_selector =
        [data_selection](const mofka::Metadata&, const mofka::DataDescriptor& desc) {
        return desc.makeSubView(0, desc.size()*data_selection);
    };

    std::vector<std::vector<char>> data;

    mofka::DataBroker data_broker =
        [&data](const mofka::Metadata&, const mofka::DataDescriptor& desc) {
        if(desc.size() == 0) return mofka::Data{};
        data.resize(data.size()+1);
        auto& v = data.back();
        v.resize(desc.size());
        return mofka::Data{v.data(), v.size()};
    };

    MPI_Barrier(MPI_COMM_WORLD);
    spdlog::info("Creating consumer");
#if 0
    auto consumer = topic.consumer(
            consumer_name, thread_pool, batch_size,
            data_selector, data_broker, my_partitions);
#endif
    auto consumer = topic.consumer(
            consumer_name, mofka::ThreadCount{(size_t)threads}, batch_size,
            data_selector, data_broker, my_partitions);

    spdlog::info("Start consuming events...");
    int i = 0;
    auto t_start = std::chrono::high_resolution_clock::now();
    double t_ack = 0;

    size_t message_size = 0;

    while (num_partitions) {
        auto event = consumer.pull().wait();
        if (i == 0) {
            spdlog::info("First event pulled has metadata size {}", event.metadata().string().size());
            message_size = event.metadata().string().size() + event.data().size();
        }
        if (i == warmup_events) {
            t_start = std::chrono::high_resolution_clock::now();
        }
        if (event.id() == mofka::NoMoreEvents) {
            //num_partitions -= 1;
            break;
            //continue;
        }
        ++i;
        if (acknowledge_every && i % acknowledge_every.value() == 0) {
            auto t1 = std::chrono::high_resolution_clock::now();
            event.acknowledge();
            auto t2 = std::chrono::high_resolution_clock::now();
            if(num_events >= warmup_events)
                t_ack += std::chrono::duration<double>(t2 - t1).count();
        }
        if (i % 100000 == 0 && rank == 0)
            std::cout << "Process " << rank << " consumed " << i << " messages" << std::endl;
        if (i == num_events + warmup_events)
            break;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    auto t_end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(t_end - t_start).count();
    spdlog::info("Consuming {} events took {} seconds (including {} seconds of ack time)",
                 i, elapsed, t_ack);
    spdlog::info("AGGREGATE CONSUMPTION RATE:\t {0:.2f} events/sec,\t {1:.2f} MB/sec",
                 ((size_t)i)*((size_t)num_consumers)/elapsed,
                 ((size_t)i)*((size_t)num_consumers)*message_size/(elapsed*1024*1024));

}

/**
 * @brief Main "consume" function.
 *
 * @param argc argc-1 from main.
 * @param argv argv+1 from main.
 */
static void consume(int argc, char** argv) {

    auto backend_name = std::string{argv[1]};
    if(backend_name != "mofka" && backend_name != "kafka" && backend_name != "rdkafka")
        throw mofka::Exception{"Backend name should be either \"mofka\", \"kafka\", or \"rdkafka\""};

    TCLAP::CmdLine cmd("MOFKA/Kafka CLI", ' ', "1.0");
    TCLAP::ValueArg<std::string> bootstrapArg("b", "bootstrap-file", "Bootstrap file or server",
                                              true, "", "string");
    TCLAP::ValueArg<int> eventsArg("n", "num-events", "Number of events", false, 0, "int");
    TCLAP::ValueArg<std::string> topicArg("t", "topic-name", "Topic name", true, "", "string");
    TCLAP::ValueArg<std::string> consumerArg("c", "consumer-name", "Consumer name", true, "", "string");
    TCLAP::ValueArg<int> threadsArg("p", "threads", "Number of threads", false, 0, "int");
    TCLAP::ValueArg<int> batchSizeArg("s", "batch-size", "Batch size", false, 0, "int");
    TCLAP::ValueArg<int> ackEveryArg("a", "ack-every", "Acknowledge/commit every N events", false, 0, "int");
    TCLAP::ValueArg<double> dataSelectionArg("d", "data-selection", "Data selection", false, 0.0, "double");
    TCLAP::ValueArg<int> warmupArg("w", "num-warmup-events", "Number of warmup events", false, 0, "int");

    cmd.add(bootstrapArg);
    cmd.add(topicArg);
    cmd.add(eventsArg);
    cmd.add(consumerArg);
    cmd.add(threadsArg);
    cmd.add(batchSizeArg);
    cmd.add(ackEveryArg);
    cmd.add(dataSelectionArg);
    cmd.add(warmupArg);
    cmd.parse(argc - 1, argv + 1);

    auto bootstrap_file = bootstrapArg.getValue();
    mofka::BatchSize batch_size = batchSizeArg.isSet() ?
        mofka::BatchSize{(size_t)batchSizeArg.getValue()}
        : mofka::BatchSize::Adaptive();
    std::optional<int> ack_every = ackEveryArg.isSet() ? std::optional{ackEveryArg.getValue()} : std::nullopt;

    spdlog::info("=======================================================");
    spdlog::info("Consumer will be executed with the following parameters");
    spdlog::info("  backend: {}", backend_name);
    spdlog::info("  bootstrap: {}", bootstrap_file);
    spdlog::info("  topic: {}", topicArg.getValue());
    spdlog::info("  threads: {}", threadsArg.getValue());
    spdlog::info("  batch size: {}", batch_size.value);
    spdlog::info("  data selection ratio: {}", dataSelectionArg.getValue());
    if(ackEveryArg.isSet())
        spdlog::info("  acknowledge every: {}", ackEveryArg.getValue());
    else
        spdlog::info("  acknowledge every: never");
    spdlog::info("  warmup events: {}", warmupArg.getValue());
    spdlog::info("=======================================================");

    if(backend_name == "mofka") {
        //if(eventsArg.isSet())
        //    spdlog::warn("-n/--num-events is ignored (all the messages will be consumed");
        auto driver = mofka::MofkaDriver{bootstrap_file, true};
        consume(driver,
                consumerArg.getValue(),
                topicArg.getValue(),
                threadsArg.getValue(),
                batch_size,
                dataSelectionArg.getValue(),
                ack_every,
                warmupArg.getValue(),
                eventsArg.getValue());
    } else if(backend_name == "kafka") {
        if(eventsArg.isSet())
            spdlog::warn("-n/--num-events is ignored (all the messages will be consumed");
        auto driver = mofka::KafkaDriver{bootstrap_file};
        consume(driver,
                consumerArg.getValue(),
                topicArg.getValue(),
                threadsArg.getValue(),
                batch_size,
                dataSelectionArg.getValue(),
                ack_every,
                warmupArg.getValue(),
                eventsArg.getValue());
    } else { /* rdkafka */
        rdkafka_consume_messages(
                bootstrapArg.getValue(),
                consumerArg.getValue(),
                topicArg.getValue(),
                batch_size,
                eventsArg.getValue(),
                ack_every,
                warmupArg.getValue());
    }
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::info);

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    spdlog::set_level(rank == 0 ? spdlog::level::info : spdlog::level::err);

    if(argc < 3)
        throw mofka::Exception{"Invalid number of arguments"};

    auto argv1 = std::string{argv[1]};
    if(argv1 == "produce") {
        produce(argc-1, argv+1);
    } else if(argv1 == "consume") {
        consume(argc-1, argv+1);
    } else if(argv1 == "help") {
        std::cerr << "Please run " << argv[0] << " produce help or "
                                   << argv[0] << " consume help" << std::endl;
    } else {
        std::cerr << "Invalid command (expected \"produce\" or \"consume\" or \"help\")";
    }

    MPI_Finalize();

    return 0;
}

