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

    spdlog::info("Topic {} created successfully!", topic_name);
}

/**
 * @brief Produce messages into a Kafka topic using librdkafka.
 *
 * @param bootstrap_servers Comma-separated list of bootstrap server addresses.
 * @param topic_name Name of the topic in which to produce events.
 * @param num_events Number of events.
 * @param message_size Size of each message.
 * @param warmup_events Number of warmup events to produce before actually measuring performance.
 * @param flush_every Frequency of flush calls.
 */
static void rdkafka_produce_messages(
        const std::string &bootstrap_servers,
        const std::string &topic_name,
        int num_events,
        int message_size,
        int warmup_events,
        int flush_every) {
    rdkafka_create_topic(bootstrap_servers, topic_name, 1, 1);

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), nullptr, 0);
    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, nullptr, 0);

    int total_num_events = warmup_events + num_events;
    spdlog::info("Preparring {} messages...", total_num_events);
    std::vector<std::string> messages(total_num_events);
    for(auto& msg : messages)
        msg = random_bytes(message_size);

    decltype(std::chrono::high_resolution_clock::now()) t_start;
    spdlog::info("Producing {} messages...", total_num_events);

    double flush_time = 0.0;

    for (int i = 0; i < total_num_events; ++i) {
        if (i == warmup_events)
            t_start = std::chrono::high_resolution_clock::now();
        /* Note: technically because we have created the messages above and they won't
         * disappear from memory, we could avoid using RD_KAFKA_MSG_F_COPY bellow. 
         * However for a more fair comparison with Mofka, which makes a copy, we use
         * RD_KAFKA_MSG_F_COPY here.
         * */
        rd_kafka_producev(producer,
                          RD_KAFKA_V_TOPIC(topic_name.c_str()),
                          RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                          RD_KAFKA_V_VALUE(const_cast<char*>(messages[i].data()), messages[i].size()),
                          RD_KAFKA_V_END);

        rd_kafka_poll(producer, 0);

        if (flush_every > 0 && (i + 1) % flush_every == 0) {
            auto t_flush_start = std::chrono::high_resolution_clock::now();
            while(RD_KAFKA_RESP_ERR__TIMED_OUT == rd_kafka_flush(producer, 100)) {};
            auto t_flush_end = std::chrono::high_resolution_clock::now();
            flush_time += std::chrono::duration<double>(t_flush_end - t_flush_start).count();
        }
    }
    auto t_flush_start = std::chrono::high_resolution_clock::now();
    while(RD_KAFKA_RESP_ERR__TIMED_OUT == rd_kafka_flush(producer, 100)) {};
    auto t_flush_end = std::chrono::high_resolution_clock::now();
    flush_time += std::chrono::duration<double>(t_flush_end - t_flush_start).count();
    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    spdlog::info("Successfully produced {} messages in {} seconds (including {} seconds flush time)",
                 num_events, elapsed, flush_time);

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
        int num_events,
        std::optional<int> acknowledge_every,
        int warmup_events) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "group.id", consumer_name.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", nullptr, 0);

    rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, nullptr, 0);
    rd_kafka_poll_set_consumer(consumer);

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic_name.c_str(), -1);
    rd_kafka_subscribe(consumer, topics);

    rd_kafka_subscribe(consumer, topics);

    decltype(std::chrono::high_resolution_clock::now()) t_start;

    spdlog::info("Consuming {} messages...", warmup_events + num_events);
    int i = 0;
    double commit_time = 0.0;
    while(true) {
        if (i == warmup_events)
            t_start = std::chrono::high_resolution_clock::now();

        rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer, 100);

        if (msg && !msg->err) {
            if (i >= warmup_events) {
                if (acknowledge_every.has_value() && (i - warmup_events + 1) % acknowledge_every.value() == 0) {
                    auto t_commit_start = std::chrono::high_resolution_clock::now();
                    rd_kafka_commit_message(consumer, msg, 0);
                    auto t_commit_end = std::chrono::high_resolution_clock::now();
                    commit_time += std::chrono::duration<double>(t_commit_end - t_commit_start).count();
                }
            }
            rd_kafka_message_destroy(msg);
        }

        if (i == warmup_events + num_events) break;
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    spdlog::info("Successfully consumed {} messages in {} seconds (including {} seconds commit time)",
                 num_events, elapsed, commit_time);

    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);
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
    std::vector<std::string> metadata(total_num_events);
    for (int i = 0; i < total_num_events; ++i) {
        data[i] = random_bytes(data_size);
        metadata[i] = "\"" + random_bytes(metadata_size-2) + "\"";
    }

    spdlog::info("Creating producer");
    auto producer = topic.producer(batch_size, thread_pool, ordering);

    if (warmup_events > 0) {
        spdlog::info("Start producing {} for warmup...", warmup_events);
        for (int i = 0; i < warmup_events; ++i) {
            producer.push(std::move(metadata[i]), mofka::Data{data[i].data(), data[i].size()});
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

    spdlog::info("Start producing {} events...", num_events);
    auto t_start = std::chrono::high_resolution_clock::now();
    double t_flush = 0;
    for (int i = 0; i < num_events; ++i) {
        int j = i + warmup_events;
        producer.push(metadata[j],mofka::Data{data[j].data(), data[j].size()});
        if (flush_every && (j + 1) % flush_every == 0) {
            auto t_flush_start = std::chrono::high_resolution_clock::now();
            producer.flush();
            auto t_flush_end = std::chrono::high_resolution_clock::now();
            t_flush += std::chrono::duration<double>(t_flush_end - t_flush_start).count();
        }
    }
    auto t_flush_start = std::chrono::high_resolution_clock::now();
    producer.flush();
    auto t_flush_end = std::chrono::high_resolution_clock::now();
    t_flush += std::chrono::duration<double>(t_flush_end - t_flush_start).count();
    auto t_end = std::chrono::high_resolution_clock::now();
    spdlog::info("Marking topic as complete");
    topic.markAsComplete();
    spdlog::info("Producing {} events took {} seconds (including {} seconds flush time)", num_events,
                 std::chrono::duration<double>(t_end - t_start).count(), t_flush);
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
    TCLAP::ValueArg<int> eventsArg("n", "num-events", "Number of events", true, 0, "int");
    TCLAP::ValueArg<int> threadsArg("p", "threads", "Number of threads", false, 0, "int");
    TCLAP::ValueArg<int> batchSizeArg("s", "batch-size", "Batch size", false, 0, "int");
    TCLAP::ValueArg<int> flushEveryArg("f", "flush-every", "Flush every N events", false, 0, "int");
    TCLAP::ValueArg<int> metadataSizeArg("m", "metadata-size", "Metadata size", false, 8, "int");
    TCLAP::ValueArg<int> dataSizeArg("d", "data-size", "Data size", false, 16, "int");
    TCLAP::ValueArg<int> warmupArg("w", "num-warmup-events", "Number of warmup events", false, 0, "int");

    cmd.add(bootstrapArg);
    cmd.add(topicArg);
    cmd.add(eventsArg);
    cmd.add(threadsArg);
    cmd.add(batchSizeArg);
    cmd.add(flushEveryArg);
    cmd.add(metadataSizeArg);
    cmd.add(dataSizeArg);
    cmd.add(warmupArg);
    cmd.parse(argc - 1, argv + 1);

    auto bootstrap_file = bootstrapArg.getValue();
    mofka::BatchSize batch_size = batchSizeArg.isSet() ?
        mofka::BatchSize{(size_t)batchSizeArg.getValue()}
        : mofka::BatchSize::Adaptive();

    if(backend_name == "mofka") {
        auto driver = mofka::MofkaDriver{bootstrap_file, true};
        margo_set_progress_when_needed(driver.engine().get_margo_instance(), true);
        driver.createTopic(topicArg.getValue());
        driver.addMemoryPartition(topicArg.getValue(), 0);
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
        driver.createTopic(topicArg.getValue(), 1, 1);
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
        rdkafka_produce_messages(
            bootstrapArg.getValue(),
            topicArg.getValue(),
            eventsArg.getValue(),
            dataSizeArg.getValue() + metadataSizeArg.getValue(),
            warmupArg.getValue(),
            flushEveryArg.getValue());
    }
}

template <typename Driver>
static void consume(Driver driver, const std::string& consumer_name, const std::string& topic_name,
                    int threads, mofka::BatchSize batch_size, double data_selection,
                    std::optional<int> acknowledge_every, int warmup_events) {
    spdlog::info("Opening topic {}", topic_name);
    auto topic = driver.openTopic(topic_name);

    if (data_selection < 0 || data_selection > 1) {
        throw std::invalid_argument("data_selection should be in [0,1]");
    }

    auto thread_pool = mofka::ThreadPool{mofka::ThreadCount{(size_t)threads}};

    mofka::DataSelector data_selector =
        [data_selection](const mofka::Metadata&, const mofka::DataDescriptor& desc) {
        return desc.makeSubView(0, desc.size()*data_selection);
    };

    mofka::DataBroker data_broker =
        [](const mofka::Metadata&, const mofka::DataDescriptor& desc) {
        if(desc.size() == 0) return mofka::Data{};
        return mofka::Data{new char[desc.size()], desc.size()};
    };

    spdlog::info("Creating consumer");
    auto consumer = topic.consumer(
            consumer_name, thread_pool, batch_size, data_selector, data_broker);

    spdlog::info("Start consuming events...");
    int num_events = 0;
    auto t_start = std::chrono::high_resolution_clock::now();

    while (true) {
        auto event = consumer.pull().wait();
        if (num_events == warmup_events) {
            t_start = std::chrono::high_resolution_clock::now();
        }
        if (event.id() == mofka::NoMoreEvents) break;
        ++num_events;
        if (acknowledge_every && num_events % acknowledge_every.value() == 0) {
            event.acknowledge();
        }
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    spdlog::info("Consuming {} events took {} seconds", num_events - warmup_events,
                 std::chrono::duration<double>(t_end - t_start).count());
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

    if(backend_name == "mofka") {
        if(eventsArg.isSet())
            spdlog::warn("-n/--num-events is ignored (all the messages will be consumed");
        auto driver = mofka::MofkaDriver{bootstrap_file, true};
        margo_set_progress_when_needed(driver.engine().get_margo_instance(), true);
        consume(driver,
                consumerArg.getValue(),
                topicArg.getValue(),
                threadsArg.getValue(),
                batch_size,
                dataSelectionArg.getValue(),
                ack_every,
                warmupArg.getValue());
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
                warmupArg.getValue());
    } else { /* rdkafka */
        rdkafka_consume_messages(
                bootstrapArg.getValue(),
                consumerArg.getValue(),
                topicArg.getValue(),
                eventsArg.getValue(),
                ack_every,
                warmupArg.getValue());
    }
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::info);

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

    return 0;
}

