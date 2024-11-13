#include <mofka/MofkaDriver.hpp>
#include <mofka/TopicHandle.hpp>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 3) {
        std::cerr << "Usage: "
                  << argv[0] << " <groupfile> <topic>" << std::endl;
        return -1;
    }

    auto group_file = argv[1];
    auto topic_name = argv[2];

    try {

        mofka::MofkaDriver driver{group_file, true};

        mofka::TopicHandle topic = driver.openTopic(topic_name);

        mofka::Producer producer = topic.producer();
        for(size_t i = 0; i < 100; ++i) {
            auto future = producer.push(
                mofka::Metadata{R"({"x":42,"name":"bob"})"},
                mofka::Data{}
            );
            // auto event_id = future.wait();
        }
        producer.flush();

    } catch(const mofka::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }
    return 0;
}
