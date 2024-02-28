#include <mofka/PartitionSelector.hpp>
#include <fmt/format.h>


class EnergyPartitionSelector final : public mofka::PartitionSelectorInterface {

    const size_t                      energy_max;
    std::vector<mofka::PartitionInfo> m_targets;

    public:

    EnergyPartitionSelector(size_t _energy_max)
    : energy_max(_energy_max) {}

    void setPartitions(const std::vector<mofka::PartitionInfo>& targets) override {
        m_targets = targets;
    }

    mofka::PartitionInfo selectPartitionFor(const mofka::Metadata& metadata) override {
        auto energy = metadata.json()["energy"].get<size_t>();
        auto i = energy*m_targets.size()/energy_max;
        return m_targets[i];
    }

    mofka::Metadata metadata() const override {
        return mofka::Metadata{};
    }

    static std::unique_ptr<mofka::PartitionSelectorInterface> create(
            const mofka::Metadata& metadata) {
        if(!metadata.json().is_object())
            throw mofka::InvalidMetadata{
                "EnergyPartitionSelector configuration should be a JSON object"};
        if(!metadata.json().contains("energy_max"))
            throw mofka::InvalidMetadata{
                "EnergyPartitionSelector configuration should contain an \"energy_max\" field"};
        if(!metadata.json()["energy_max"].is_number_integer())
            throw mofka::InvalidMetadata{
                "EnergyPartitionSelector configuration's energy_max field should be an integer"};
        return std::make_unique<EnergyPartitionSelector>(metadata.json()["energy_max"].get<size_t>());
    }

};

MOFKA_REGISTER_PARTITION_SELECTOR(energy_partition_selector, EnergyPartitionSelector);
