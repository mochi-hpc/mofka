#include <mofka/MofkaDriver.hpp>
#include <mofka/TopicHandle.hpp>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 2) {
        std::cerr << "Usage: "
                  << argv[0] << " <groupfile>" << std::endl;
        return -1;
    }

    auto group_file = argv[1];

    try {

        mofka::MofkaDriver driver{group_file, true};

        // START CREATE TOPIC
        mofka::Validator validator =
            mofka::Validator::FromMetadata(
                "energy_validator:libenergy_validator.so",
                mofka::Metadata{{{"energy_max", 100}}}
            );

        mofka::PartitionSelector selector =
            mofka::PartitionSelector::FromMetadata(
                "energy_partition_selector:libenergy_partition_selector.so",
                mofka::Metadata{{{"energy_max", 100}}}
            );

        mofka::Serializer serializer =
            mofka::Serializer::FromMetadata(
                "energy_serializer:libenergy_serializer.so",
                mofka::Metadata{{{"energy_max", 100}}}
            );

        driver.createTopic("collisions", validator, selector, serializer);
        // END CREATE TOPIC

        // START ADD PARTITION
        // add an in-memory partition
        driver.addMemoryPartition("collisions", 0);
        // add a default partition (all arguments specified)
        driver.addDefaultPartition(
                "collisions", 0,
                "my_metadata_provider@local",
                "my_data_provider@local",
                {}, "__primary__");
        // add a default partition (discover providers automatically)
        driver.addDefaultPartition("collisions", 0);
        // END ADD PARTITION

        {
        // START PRODUCER
        mofka::TopicHandle topic = driver.openTopic("collisions");

        mofka::ThreadPool thread_pool{mofka::ThreadCount{4}};
        mofka::BatchSize  batch_size = mofka::BatchSize::Adaptive();
        mofka::Ordering   ordering = mofka::Ordering::Loose; // or Strict

        mofka::Producer producer = topic.producer(
                "app1", thread_pool, batch_size, ordering);
        // END PRODUCER

        // START EVENT
        std::vector<char> segment1 = { 'a', 'b', 'c', 'd' };

        // expose 1 segment using its pointer and size
        mofka::Data data1{segment1.data(), segment1.size()};

        std::vector<char> segment2 = { 'e', 'f' };

        // expose 2 non-contiguous segments using mofka::Data::Segment
        mofka::Data data2{{
            mofka::Data::Segment{segment1.data(), segment1.size()},
            mofka::Data::Segment{segment2.data(), segment2.size()}
        }};
        mofka::Metadata metadata1{R"({"energy": 42})"};

        using json = nlohmann::json;
        auto md = json::object();
        md["energy"] = 42;
        mofka::Metadata metadata2{md};
        // END EVENT

        // START PRODUCE EVENT
        mofka::Future<mofka::EventID> future = producer.push(metadata1, data1);
        future.completed(); // returns true if the future has completed

        producer.push(metadata2, data2);

        mofka::EventID event_id_1 = future.wait();

        producer.flush();
        // END PRODUCE EVENT
        }

        {
        // START CONSUMER
        mofka::TopicHandle topic = driver.openTopic("collisions");
        mofka::BatchSize  batch_size = mofka::BatchSize::Adaptive();
        mofka::ThreadPool thread_pool{mofka::ThreadCount{4}};
        mofka::DataSelector data_selector =
            [](const mofka::Metadata& md, const mofka::DataDescriptor& dd) -> mofka::DataDescriptor {
                if(md.json()["energy"] > 20) {
                    return dd;
                } else {
                    return mofka::DataDescriptor::Null();
                }
            };
        mofka::DataBroker data_broker =
            [](const mofka::Metadata& md, const mofka::DataDescriptor& dd) -> mofka::Data {
                char* ptr = new char[dd.size()];
                return mofka::Data{ptr, dd.size()};
            };
        mofka::Consumer consumer = topic.consumer(
            "app2", thread_pool, batch_size, data_selector, data_broker);
        // END CONSUMER
        // START CONSUME EVENTS
        mofka::Future<mofka::Event> future = consumer.pull();
        future.completed(); // returns true if the future has completed

        mofka::Event event        = future.wait();
        mofka::Data data          = event.data();
        mofka::Metadata metadata  = event.metadata();
        mofka::EventID event_id   = event.id();

        event.acknowledge();

        delete[] static_cast<char*>(data.segments()[0].ptr);
        // END CONSUME EVENTS
        }

    } catch(const mofka::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    return 0;
}
