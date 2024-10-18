#include <mofka/MofkaDriver.hpp>
#include <mofka/TopicHandle.hpp>
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

    try {

        mofka::MofkaDriver driver{group_file, engine};

        mofka::TopicHandle topic = driver.openTopic(topic_name);

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
