#include <mofka/Serializer.hpp>
#include <mofka/Metadata.hpp>

class MySerializer : public mofka::SerializerInterface {

    public:

    void serialize(mofka::Archive& archive, const mofka::Metadata& metadata) const override {
        const auto& str = metadata.string();
        size_t s = str.size();
        archive.write(&s, sizeof(s));
        archive.write(str.data(), s);
    }

    void deserialize(mofka::Archive& archive, mofka::Metadata& metadata) const override {
        auto& str = metadata.string();
        size_t s = 0;
        archive.read(&s, sizeof(s));
        str.resize(s);
        archive.read(const_cast<char*>(str.data()), s);
    }

    mofka::Metadata metadata() const override {
        return mofka::Metadata{"{\"type\":\"my_serializer:libmy_serializer.so\"}"};
    }

    static std::unique_ptr<mofka::SerializerInterface> create(const mofka::Metadata& metadata) {
        (void)metadata;
        return std::make_unique<MySerializer>();
    }
};
using namespace mofka;
MOFKA_REGISTER_SERIALIZER(my_serializer, MySerializer);
