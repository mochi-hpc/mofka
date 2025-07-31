#include <diaspora/Validator.hpp>
#include <fmt/format.h>


class EnergyValidator final : public diaspora::ValidatorInterface {

    const size_t energy_max;

    public:

    EnergyValidator(size_t _energy_max)
    : energy_max(_energy_max) {}

    void validate(const diaspora::Metadata& metadata, const diaspora::DataView& data) const override {
        if(!metadata.json().is_object())
            throw diaspora::InvalidMetadata{
                "EnergyValidator expects metadata to be a JSON object"};
        if(!metadata.json().contains("energy"))
            throw diaspora::InvalidMetadata{
                "EnergyValidator expects metadata to contain an \"energy\" field"};
        if(!metadata.json()["energy"].is_number_integer())
            throw diaspora::InvalidMetadata{
                "EnergyValidator expects x_max field to be an integer"};
        if(metadata.json()["energy"].get<size_t>() >= energy_max)
            throw diaspora::InvalidMetadata{
                fmt::format("EnergyValidator expects energy value to be lower than {}", energy_max)};
        (void)data; // the validator could also validate the content of the data
    }

    diaspora::Metadata metadata() const override {
        return diaspora::Metadata{
            {{"energy_max", energy_max}}
        };
    }

    static std::unique_ptr<diaspora::ValidatorInterface> create(const diaspora::Metadata& metadata) {
        if(!metadata.json().is_object())
            throw diaspora::InvalidMetadata{
                "EnergyValidator configuration should be a JSON object"};
        if(!metadata.json().contains("energy_max"))
            throw diaspora::InvalidMetadata{
                "EnergyValidator configuration should contain an \"energy_max\" field"};
        if(!metadata.json()["energy_max"].is_number_integer())
            throw diaspora::InvalidMetadata{
                "EnergyValidator configuration's energy_max field should be an integer"};
        return std::make_unique<EnergyValidator>(metadata.json()["energy_max"].get<size_t>());
    }

};

DIASPORA_REGISTER_VALIDATOR(_, energy_validator, EnergyValidator);
