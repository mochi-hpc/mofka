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


std::string random_bytes(size_t n) {
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

void rdkafka_create_topic(
        const std::string &bootstrap_servers,
        const std::string &topic_name,
        int num_partitions,
        int replication_factor) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, nullptr, 0);
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), nullptr, 0);

    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ANY);
    rd_kafka_NewTopic_t *new_topic = rd_kafka_NewTopic_new(topic_name.c_str(), num_partitions, replication_factor, nullptr, 0);

    rd_kafka_NewTopic_t *new_topics[] = {new_topic};
    rd_kafka_CreateTopics(rk, new_topics, 1, options, nullptr);

    rd_kafka_NewTopic_destroy(new_topic);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_destroy(rk);

    spdlog::info("Topic {} created successfully!", topic_name);
}

void rdkafka_produce_messages(
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
    auto t_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < total_num_events; ++i) {
        std::string message = random_bytes(message_size);
        rd_kafka_producev(producer,
                          RD_KAFKA_V_TOPIC(topic_name.c_str()),
                          RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                          RD_KAFKA_V_VALUE(const_cast<char*>(message.data()), message.size()),
                          RD_KAFKA_V_END);

        rd_kafka_poll(producer, 0);

        if (flush_every > 0 && (i + 1) % flush_every == 0) {
            rd_kafka_flush(producer, 1000);
        }

        if (i == warmup_events) {
            t_start = std::chrono::high_resolution_clock::now();
        }
    }

    rd_kafka_flush(producer, 1000);
    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    spdlog::info("Successfully produced {} messages in {} seconds", num_events, elapsed);

    rd_kafka_destroy(producer);
}

void rdkafka_consume_messages(
        const std::string &bootstrap_servers,
        const std::string &consumer_name,
        const std::string &topic_name,
        int num_events,
        int acknowledge_every,
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

    int i = 0;
    auto t_start = std::chrono::high_resolution_clock::now();

    while (true) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer, 1000);

        if (msg && !msg->err) {
            if (i >= warmup_events) {
                if (i == warmup_events) {
                    t_start = std::chrono::high_resolution_clock::now();
                }
                if (acknowledge_every > 0 && (i - warmup_events + 1) % acknowledge_every == 0) {
                    rd_kafka_commit_message(consumer, msg, 0);
                }
                ++i;
            }
            rd_kafka_message_destroy(msg);
        }
        if (i == num_events) break;
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    spdlog::info("Successfully consumed {} messages in {} seconds", num_events, elapsed);

    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);
}


template <typename Driver>
void produce(Driver driver, const std::string& topic_name, int num_events,
             int threads, mofka::BatchSize batch_size, int flush_every,
             int metadata_size, int data_size, int warmup_events) {
    spdlog::info("Opening topic {}", topic_name);
    auto topic = driver.openTopic(topic_name);

    auto thread_pool = mofka::ThreadPool{mofka::ThreadCount{(size_t)threads}};
    auto ordering = mofka::Ordering::Strict;
    int total_num_events = warmup_events + num_events;

    std::vector<std::string> data(total_num_events);
    std::vector<std::string> metadata(total_num_events);
    for (int i = 0; i < total_num_events; ++i) {
        data[i] = random_bytes(data_size);
        metadata[i] = "\"" + random_bytes(metadata_size) + "\"";
    }

    spdlog::info("Creating producer");
    auto producer = topic.producer(batch_size, thread_pool, ordering);

    if (warmup_events > 0) {
        spdlog::info("Start producing {} for warmup...", warmup_events);
        for (int i = 0; i < warmup_events; ++i) {
            producer.push(metadata[i], mofka::Data{data[i].data(), data[i].size()});
        }
        producer.flush();
    }

    spdlog::info("Start producing {}...", num_events);
    auto t_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_events; ++i) {
        int j = i + warmup_events;
        producer.push(metadata[j],mofka::Data{data[j].data(), data[j].size()});
        if (flush_every && (j + 1) % flush_every == 0) {
            producer.flush();
        }
    }
    producer.flush();
    auto t_end = std::chrono::high_resolution_clock::now();
    spdlog::info("Marking topic as complete");
    topic.markAsComplete();
    spdlog::info("Producing {} events took {} seconds", num_events,
                 std::chrono::duration<double>(t_end - t_start).count());
}

void produce(int argc, char** argv) {

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
        auto driver = mofka::MofkaDriver{bootstrap_file};
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
                warmupArg.getValue());
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
    } else {
        rdkafka_produce_messages(
            bootstrapArg.getValue(),
            topicArg.getValue(),
            eventsArg.getValue(),
            dataSizeArg.getValue() + metadataSizeArg.getValue(),
            warmupArg.getValue(),
            flushEveryArg.getValue());
    }
}

#if 0
template <typename Driver>
void consume(Driver driver, const std::string& consumer_name, const std::string& topic_name,
             int threads, std::optional<int> batch_size, double data_selection,
             std::optional<int> acknowledge_every, int warmup_events) {
    spdlog::info("Opening topic {}", topic_name);
    auto topic = driver.openTopic(topic_name);

    if (data_selection < 0 || data_selection > 1) {
        throw std::invalid_argument("data_selection should be in [0,1]");
    }

    auto thread_pool = mofka::ThreadPool{threads};
    auto actual_batch_size = mofka::BatchSize{
        batch_size.value_or(mofka::BatchSize::Adaptive().value)};

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
            consumer_name, thread_pool, actual_batch_size, data_selector, data_broker);

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
#endif

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::info);

    if(argc < 3)
        throw mofka::Exception{"Invalid number of arguments"};

    auto argv1 = std::string{argv[1]};
    if(argv1 == "produce") {
        produce(argc-1, argv+1);
    } else if(argv1 == "consume") {

    } else {
        throw mofka::Exception{"Invalid command (expected \"produce\" or \"consume\")"};
    }

    return 0;
}

