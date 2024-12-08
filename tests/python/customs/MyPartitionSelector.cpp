#include <mofka/PartitionSelector.hpp>
#include <mofka/Metadata.hpp>


class MyPartitionSelector : public mofka::PartitionSelectorInterface {

    public:

    void setPartitions(const std::vector<mofka::PartitionInfo>& targets) override {
        m_targets = targets;
    }

    size_t selectPartitionFor(const mofka::Metadata& metadata, std::optional<size_t> requested) override {
        (void)metadata;
        if(m_targets.size() == 0)
            throw mofka::Exception("PartitionSelector has no target to select from");
        if(requested.has_value()) {
            size_t req = requested.value();
            if(req >= m_targets.size()) {
                throw mofka::Exception("Requested partition is out of range");
            }
        }
        auto ret = m_index;
        m_index += 1;
        m_index %= m_targets.size();
        return ret;
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
