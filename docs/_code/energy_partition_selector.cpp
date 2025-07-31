#include <diaspora/PartitionSelector.hpp>
#include <diaspora/InvalidMetadata.hpp>
#include <fmt/format.h>


class EnergyPartitionSelector final : public diaspora::PartitionSelectorInterface {

    const size_t                      energy_max;
    std::vector<diaspora::PartitionInfo> m_targets;

    public:

    EnergyPartitionSelector(size_t _energy_max)
    : energy_max(_energy_max) {}

    void setPartitions(const std::vector<diaspora::PartitionInfo>& targets) override {
        m_targets = targets;
    }

    size_t selectPartitionFor(const diaspora::Metadata& metadata, std::optional<size_t> requested) override {
        if(requested.has_value()) {
           if(m_targets.size() >= *requested) {
               throw diaspora::Exception("Invalid requested partition number");
           } else {
               return *requested;
           }
        }
        auto energy = metadata.json()["energy"].get<size_t>();
        auto i = energy*m_targets.size()/energy_max;
        return i;
    }

    diaspora::Metadata metadata() const override {
        return diaspora::Metadata{};
    }

    static std::unique_ptr<diaspora::PartitionSelectorInterface> create(
            const diaspora::Metadata& metadata) {
        if(!metadata.json().is_object())
            throw diaspora::InvalidMetadata{
                "EnergyPartitionSelector configuration should be a JSON object"};
        if(!metadata.json().contains("energy_max"))
            throw diaspora::InvalidMetadata{
                "EnergyPartitionSelector configuration should contain an \"energy_max\" field"};
        if(!metadata.json()["energy_max"].is_number_integer())
            throw diaspora::InvalidMetadata{
                "EnergyPartitionSelector configuration's energy_max field should be an integer"};
        return std::make_unique<EnergyPartitionSelector>(metadata.json()["energy_max"].get<size_t>());
    }

};

DIASPORA_REGISTER_PARTITION_SELECTOR(_, energy_partition_selector, EnergyPartitionSelector);
