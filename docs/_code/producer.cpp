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
        mofka::ServiceHandle sh = client.connect(group_file);

        sh.createTopic(topic_name);
        sh.addMemoryPartition(topic_name, 0);

        mofka::TopicHandle topic = sh.openTopic(topic_name);

        mofka::Producer producer = topic.producer();
        for(size_t i = 0; i < 100; ++i) {
            producer.push(
                mofka::Metadata{R"({"x":42,"name":"bob"})"},
                mofka::Data{}
            );
        }
        producer.flush();

    } catch(const mofka::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    engine.finalize();
    return 0;
}
