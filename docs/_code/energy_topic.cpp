#include <diaspora/Driver.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/TopicHandle.hpp>
#include <mofka/MofkaDriver.hpp>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 2) {
        std::cerr << "Usage: "
                  << argv[0] << " <groupfile>" << std::endl;
        return -1;
    }

    auto group_file = argv[1];

    try {

        diaspora::Metadata options;
        options.json()["group_file"] = group_file;
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;

        diaspora::Driver driver = diaspora::Driver::New("mofka", options);

        // START CREATE TOPIC
        diaspora::Validator validator =
            diaspora::Validator::FromMetadata(
                diaspora::Metadata{{
                    {"type","energy_validator:libenergy_validator.so"},
                    {"energy_max", 100}}}
            );

        diaspora::PartitionSelector selector =
            diaspora::PartitionSelector::FromMetadata(
                diaspora::Metadata{{
                    {"type","energy_partition_selector:libenergy_partition_selector.so"},
                    {"energy_max", 100}}}
            );

        diaspora::Serializer serializer =
            diaspora::Serializer::FromMetadata(
                diaspora::Metadata{{
                    {"type","energy_serializer:libenergy_serializer.so"},
                    {"energy_max", 100}}}
            );

        driver.createTopic("collisions", diaspora::Metadata{}, validator, selector, serializer);
        // END CREATE TOPIC

        // START ADD PARTITION
        auto& diaspora_driver = driver.as<mofka::MofkaDriver>();
        // add an in-memory partition
        diaspora_driver.addMemoryPartition("collisions", 0);
        // add a default partition (all arguments specified)
        diaspora_driver.addDefaultPartition(
                "collisions", 0,
                "my_metadata_provider@local",
                "my_data_provider@local",
                {}, "__primary__");
        // add a default partition (discover providers automatically)
        diaspora_driver.addDefaultPartition("collisions", 0);
        // END ADD PARTITION

        // START ADD PROVIDERS
        // add a metadata provider
        auto metadata_config = diaspora::Metadata{R"({
            "database": {
                "type": "log",
                "config": {
                    "path": "/tmp/diaspora-log",
                    "create_if_missing": true
                }
            }
        })"};
        auto metadata_provider = diaspora_driver.addDefaultMetadataProvider(0, metadata_config);
        // add a data provider
        auto data_config = diaspora::Metadata{R"({
            "target": {
                "type": "abtio",
                "config": {
                    "path": "/tmp/diaspora-data",
                    "create_if_missing": true
                }
            }
        })"};
        auto data_provider = diaspora_driver.addDefaultDataProvider(0, data_config);
        // create a partition that uses these providers
        diaspora_driver.addDefaultPartition("collisions", 0, metadata_provider, data_provider);
        // END ADD PROVIDERS
        {
        // START PRODUCER
        diaspora::TopicHandle topic = driver.openTopic("collisions");

        diaspora::ThreadPool thread_pool = driver.makeThreadPool(diaspora::ThreadCount{4});
        diaspora::BatchSize  batch_size = diaspora::BatchSize::Adaptive();
        diaspora::Ordering   ordering = diaspora::Ordering::Loose; // or Strict

        diaspora::Producer producer = topic.producer(
                "app1", thread_pool, batch_size, ordering);
        // END PRODUCER

        // START EVENT
        std::vector<char> segment1 = { 'a', 'b', 'c', 'd' };

        // expose 1 segment using its pointer and size
        diaspora::DataView data1{segment1.data(), segment1.size()};

        std::vector<char> segment2 = { 'e', 'f' };

        // expose 2 non-contiguous segments using diaspora::Data::Segment
        diaspora::DataView data2{{
            diaspora::DataView::Segment{segment1.data(), segment1.size()},
            diaspora::DataView::Segment{segment2.data(), segment2.size()}
        }};
        diaspora::Metadata metadata1{R"({"energy": 42})"};

        using json = nlohmann::json;
        auto md = json::object();
        md["energy"] = 42;
        diaspora::Metadata metadata2{md};
        // END EVENT

        // START PRODUCE EVENT
        diaspora::Future<diaspora::EventID> future = producer.push(metadata1, data1);
        future.completed(); // returns true if the future has completed

        producer.push(metadata2, data2);

        diaspora::EventID event_id_1 = future.wait();

        producer.flush();
        // END PRODUCE EVENT
        }

        {
        // START CONSUMER
        diaspora::TopicHandle topic = driver.openTopic("collisions");
        diaspora::BatchSize  batch_size = diaspora::BatchSize::Adaptive();
        diaspora::ThreadPool thread_pool = driver.defaultThreadPool();
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata& md, const diaspora::DataDescriptor& dd) -> diaspora::DataDescriptor {
                if(md.json()["energy"] > 20) {
                    return dd;
                } else {
                    return diaspora::DataDescriptor{};
                }
            };
        diaspora::DataAllocator data_allocator =
            [](const diaspora::Metadata& md, const diaspora::DataDescriptor& dd) -> diaspora::DataView {
                (void)md;
                char* ptr = new char[dd.size()];
                return diaspora::DataView{ptr, dd.size()};
            };
        diaspora::Consumer consumer = topic.consumer(
            "app2", thread_pool, batch_size, data_selector, data_allocator);
        // END CONSUMER
        // START CONSUME EVENTS
        diaspora::Future<diaspora::Event> future = consumer.pull();
        future.completed(); // returns true if the future has completed

        diaspora::Event event        = future.wait();
        diaspora::DataView data      = event.data();
        diaspora::Metadata metadata  = event.metadata();
        diaspora::EventID event_id   = event.id();

        event.acknowledge();

        delete[] static_cast<char*>(data.segments()[0].ptr);
        // END CONSUME EVENTS
        }

    } catch(const diaspora::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    return 0;
}
