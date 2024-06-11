#include <mofka/Client.hpp>
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

        mofka::Client client = mofka::Client{engine};
        mofka::ServiceHandle sh = client.connect(group_file);

        mofka::TopicHandle topic = sh.openTopic(topic_name);

        mofka::Consumer consumer = topic.consumer("consumer");
        for(size_t i = 0; i < 100; ++i) {
            mofka::Event event = consumer.pull().wait();
            std::cout << event.id() << ": " << event.metadata().string() << std::endl;
            if((i+1) % 10 == 0) event.acknowledge();
        }

    } catch(const mofka::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }

    engine.finalize();
    return 0;
}
