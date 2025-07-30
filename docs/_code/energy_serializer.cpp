#include <diaspora/Serializer.hpp>
#include <fmt/format.h>


class EnergySerializer final : public diaspora::SerializerInterface {

    size_t energy_max;

    public:

    EnergySerializer(size_t _energy_max)
    : energy_max(_energy_max) {}

    void serialize(diaspora::Archive& archive, const diaspora::Metadata& metadata) const override {
        size_t energy = metadata.json()["energy"].get<size_t>();
        if(energy_max <= std::numeric_limits<uint8_t>::max()) {
            uint8_t val = static_cast<uint8_t>(energy);
            archive.write(&val, sizeof(val));
        } else if(energy_max <= std::numeric_limits<uint16_t>::max()) {
            uint16_t val = static_cast<uint16_t>(energy);
            archive.write(&val, sizeof(val));
        } else if(energy_max <= std::numeric_limits<uint32_t>::max()) {
            uint32_t val = static_cast<uint32_t>(energy);
            archive.write(&val, sizeof(val));
        } else {
            uint64_t val = static_cast<uint64_t>(energy);
            archive.write(&val, sizeof(val));
        }
    }

    void deserialize(diaspora::Archive& archive, diaspora::Metadata& metadata) const override {
        metadata = diaspora::Metadata{}; // ensure we have an empty JSON object
        if(energy_max <= std::numeric_limits<uint8_t>::max()) {
            uint8_t val;
            archive.read(&val, sizeof(val));
            metadata.json()["energy"] = val;
        } else if(energy_max <= std::numeric_limits<uint16_t>::max()) {
            uint16_t val;
            archive.read(&val, sizeof(val));
            metadata.json()["energy"] = val;
        } else if(energy_max <= std::numeric_limits<uint32_t>::max()) {
            uint32_t val;
            archive.read(&val, sizeof(val));
            metadata.json()["energy"] = val;
        } else {
            uint64_t val;
            archive.read(&val, sizeof(val));
            metadata.json()["energy"] = val;
        }
    }

    diaspora::Metadata metadata() const override {
        return diaspora::Metadata{
            {{"energy_max", energy_max}}
        };
    }

    static std::unique_ptr<diaspora::SerializerInterface> create(const diaspora::Metadata& metadata) {
        if(!metadata.json().is_object())
            throw diaspora::InvalidMetadata{
                "EnergySerializer configuration should be a JSON object"};
        if(!metadata.json().contains("energy_max"))
            throw diaspora::InvalidMetadata{
                "EnergySerializer configuration should contain an \"energy_max\" field"};
        if(!metadata.json()["energy_max"].is_number_integer())
            throw diaspora::InvalidMetadata{
                "EnergySerializer configuration's energy_max field should be an integer"};
        return std::make_unique<EnergySerializer>(metadata.json()["energy_max"].get<size_t>());
    }

};

DIASPORA_REGISTER_SERIALIZER(_, energy_serializer, EnergySerializer);
