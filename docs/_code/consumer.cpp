#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
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

        diaspora::Metadata options;
        options.json()["group_file"] = group_file;
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;

        diaspora::Driver driver = diaspora::Driver::New("mofka", options);

        diaspora::TopicHandle topic = driver.openTopic(topic_name);

        diaspora::Consumer consumer = topic.consumer("consumer");
        for(size_t i = 0; i < 100; ++i) {
            diaspora::Event event = consumer.pull().wait(-1).value();
            std::cout << event.id() << ": " << event.metadata().string() << std::endl;
            if((i+1) % 10 == 0) event.acknowledge();
        }

    } catch(const diaspora::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }
    return 0;
}
