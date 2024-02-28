#include <mofka/Client.hpp>
#include <mofka/TopicHandle.hpp>
#include <ssg.h>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 4) {
        std::cerr << "Usage: "
                  << argv[0] << " <protocol> <groupfile> <topic>" << std::endl;
        return -1;
    }

    auto protocol   = argv[1];
    auto group_file = argv[2];
    auto topic_name = argv[3];

    auto engine = thallium::engine(protocol, THALLIUM_SERVER_MODE);
    ssg_init();
    engine.push_prefinalize_callback(ssg_finalize);

    try {

        mofka::Client client = mofka::Client{engine};
        mofka::ServiceHandle sh = client.connect(mofka::SSGFileName{group_file});

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

        sh.createTopic(topic_name, validator, selector, serializer);
        // END CREATE TOPIC

        // START ADD MEMORY PARTITION
        sh.addMemoryPartition(topic_name, 0);
        // END ADD MEMORY PARTITION

        // START ADD DEFAULT PARTITION
        sh.addDefaultPartition(topic_name, 0);
        // END ADD DEFAULT PARTITION


    } catch(const mofka::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    engine.finalize();
    return 0;
}
