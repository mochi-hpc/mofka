#include <mofka/Client.hpp>
#include <mofka/TopicHandle.hpp>
#include <ssg.h>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 3) {
        std::cerr << "Usage: "
                  << argv[0] << " <protocol> <groupfile>" << std::endl;
        return -1;
    }

    auto protocol   = argv[1];
    auto group_file = argv[2];

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

        sh.createTopic("collisions", validator, selector, serializer);
        // END CREATE TOPIC

        // START ADD PARTITION
        // add an in-memory partition
        sh.addMemoryPartition("collisions", 0);
        // add a default partition (all arguments specified)
        sh.addDefaultPartition(
                "collisions", 0,
                "my_metadata_provider@local",
                "my_data_provider@local",
                {}, "__primary__");
        // add a default partition (discover providers automatically)
        sh.addDefaultPartition("collisions", 0);
        // END ADD PARTITION


    } catch(const mofka::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    engine.finalize();
    return 0;
}
