#include <mofka/PartitionSelector.hpp>
#include <mofka/Metadata.hpp>


class MyPartitionSelector : public mofka::PartitionSelectorInterface {

    public:

    void setPartitions(const std::vector<mofka::PartitionInfo>& targets) override {
        m_targets = targets;
    }

    mofka::PartitionInfo selectPartitionFor(const mofka::Metadata& metadata) override {
        (void)metadata;
        if(m_targets.size() == 0)
            throw mofka::Exception("PartitionSelector has no target to select from");
        if(m_index >= m_targets.size()) m_index = m_index % m_targets.size();
        m_index += 1;
        return m_targets.at(m_index-1);
    }

    mofka::Metadata metadata() const override {
        return mofka::Metadata{"{\"type\":\"my_partition_selector:libmy_partition_selector.so\"}"};
    }

    static std::unique_ptr<mofka::PartitionSelectorInterface> create(const mofka::Metadata& metadata) {
        (void)metadata;
        return std::make_unique<MyPartitionSelector>();
    }

    size_t                     m_index = 0;
    std::vector<mofka::PartitionInfo> m_targets;
};

using namespace mofka;
MOFKA_REGISTER_PARTITION_SELECTOR(my_partition_selector, MyPartitionSelector);
