#include <mofka/KafkaDriver.hpp>
#include <mofka/TopicHandle.hpp>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 3) {
        std::cerr << "Usage: "
                  << argv[0] << " <config-file> <topic>" << std::endl;
        return -1;
    }

    auto config_file = argv[1];
    auto topic_name  = argv[2];

    try {

        mofka::KafkaDriver driver{config_file};

        mofka::TopicHandle topic = driver.openTopic(topic_name);

        mofka::Consumer consumer = topic.consumer("consumer");
        for(size_t i = 0; i < 100; ++i) {
            mofka::Event event = consumer.pull().wait();
            std::cout << event.id() << ": " << event.metadata().string() << std::endl;
            if((i+1) % 10 == 0) event.acknowledge();
        }

    } catch(const mofka::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    return 0;
}
