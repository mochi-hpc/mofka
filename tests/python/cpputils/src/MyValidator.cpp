#include <mofka/Validator.hpp>
#include <mofka/Metadata.hpp>
#include <rapidjson/document.h>

class MyValidator : public mofka::ValidatorInterface {

    public:

    void validate(const mofka::Metadata& metadata, const mofka::Data& data) const override {
        (void)data;
        if(!metadata.isValidJson())
            throw mofka::InvalidMetadata("Metadata object does not contain valid JSON metadata");
        // const rapidjson::Document& doc = metadata.json();
        // if (!doc.HasMember("id"))
        //     throw mofka::InvalidMetadata("Metadata object must contain a id member");
    }

    mofka::Metadata metadata() const override {
        return mofka::Metadata{"{\"type\":\"my_validator:libmy_validator.so\"}"};
    }

    static std::unique_ptr<mofka::ValidatorInterface> create(const mofka::Metadata& metadata) {
        (void)metadata;
        return std::make_unique<MyValidator>();
    }
};
using namespace mofka;
MOFKA_REGISTER_VALIDATOR(my_validator, MyValidator);