#define PYBIND11_DETAILED_ERROR_MESSAGES
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include "pybind11_json/pybind11_json.hpp"
#include <mofka/Client.hpp>
#include <mofka/Data.hpp>
#include <mofka/ServiceHandle.hpp>
#include <mofka/TopicHandle.hpp>
#include <mofka/ThreadPool.hpp>
#include "../src/DataImpl.hpp"
#include "../src/JsonUtil.hpp"

#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id")
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t")

static auto get_buffer_info(const py::buffer& buf) {
    return buf.request();
}

static void check_buffer_is_contiguous(const py::buffer_info& buf_info) {
    if (!(PyBuffer_IsContiguous((buf_info).view(), 'C')
       || PyBuffer_IsContiguous((buf_info).view(), 'F')))
        throw mofka::Exception("Non-contiguous Python buffers are not yet supported");
}

static void check_buffer_is_writable(const py::buffer_info& buf_info) {
    if(buf_info.readonly) throw mofka::Exception("Python buffer is read-only");
}

static auto data_helper(const py::buffer& buffer) {
    auto buffer_info = get_buffer_info(buffer);
    check_buffer_is_contiguous(buffer_info);
    auto owner = new py::object(std::move(buffer));
    auto free_cb = [owner](mofka::Data::Context) { delete owner; };
    return mofka::Data(buffer_info.ptr, buffer_info.size, owner, std::move(free_cb));
}

static auto data_helper(const py::list& buffers) {
    std::vector<mofka::Data::Segment> segments;
    segments.reserve(buffers.size());
    for (auto buff : buffers){
        auto buff_info = get_buffer_info(buff.cast<py::buffer>());
        check_buffer_is_contiguous(buff_info);
        segments.push_back(
            mofka::Data::Segment{
                buff_info.ptr,
                static_cast<size_t>(buff_info.size)});
    }
    auto owner = new py::object(std::move(buffers));
    auto free_cb = [owner](mofka::Data::Context) { delete owner; };
    return mofka::Data(std::move(segments), owner, std::move(free_cb));
}

using PythonDataSelector = std::function<std::optional<mofka::DataDescriptor>(const nlohmann::json&, const mofka::DataDescriptor&)>;
using PythonDataBroker   = std::function<py::list(const nlohmann::json&, const mofka::DataDescriptor&)>;

PYBIND11_MODULE(pymofka_client, m) {
    m.doc() = "Python binding for the Mofka client library";

    py::register_exception<mofka::Exception>(m, "Exception", PyExc_RuntimeError);

    m.attr("AdaptiveBatchSize") = py::int_(mofka::BatchSize::Adaptive().value);

    py::class_<mofka::Validator>(m, "Validator")
        .def_static("from_metadata",
            [](const char* type, const nlohmann::json& md){
                return mofka::Validator::FromMetadata(type, md);
            }, "type"_a, "metadata"_a=nlohmann::json::object())
        .def_static("from_metadata",
            [](const nlohmann::json& md){
                return mofka::Validator::FromMetadata(md);
            }, "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::ThreadPool>(m, "ThreadPool")
        .def(py::init(
                [](std::size_t count){
                    return new mofka::ThreadPool(mofka::ThreadCount{count});
            }), "thread_count"_a=0)
        .def_property_readonly("thread_count",
            [](const mofka::ThreadPool& thread_pool) -> std::size_t {
                return thread_pool.threadCount().count;
            })
    ;

    py::enum_<mofka::Ordering>(m, "Ordering")
        .value("Strict", mofka::Ordering::Strict)
        .value("Loose", mofka::Ordering::Loose)
    ;

    py::class_<mofka::Serializer>(m, "Serializer")
        .def_static("from_metadata",
            [](const char* type, const nlohmann::json& md){
                return mofka::Serializer::FromMetadata(type, md);
            }, "type"_a, "metadata"_a=nlohmann::json::object())
        .def_static("from_metadata",
            [](const nlohmann::json& md){
                return mofka::Serializer::FromMetadata(md);
            }, "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::PartitionSelector>(m, "PartitionSelector")
        .def_static("from_metadata",
            [](const nlohmann::json& md){
                return mofka::PartitionSelector::FromMetadata(md);
            }, "metadata"_a=nlohmann::json::object())
        .def_static("from_metadata",
            [](const char* type, const nlohmann::json& md){
                return mofka::PartitionSelector::FromMetadata(type, md);
            }, "type"_a, "metadata"_a=nlohmann::json::object())
    ;

    py::class_<mofka::ServiceHandle>(m, "ServiceHandle")
        .def(py::init<const std::string&>(), "group_file"_a)
        .def(py::init<const std::string&, py_margo_instance_id>(), "group_file"_a, "mid"_a)
        .def_property_readonly("num_servers", &mofka::ServiceHandle::numServers)
        .def("create_topic",
             [](mofka::ServiceHandle& service,
                const std::string& name,
                mofka::Validator validator,
                mofka::PartitionSelector selector,
                mofka::Serializer serializer) {
                service.createTopic(name, validator, selector, serializer);
             },
             "topic_name"_a, "validator"_a=mofka::Validator{}, "selector"_a=mofka::PartitionSelector{},
             "serializer"_a=mofka::Serializer{})
        .def("open_topic",
            [](mofka::ServiceHandle& service, const std::string& name) -> mofka::TopicHandle {
                return service.openTopic(name);
            },
            "topic_name"_a)
        .def("add_default_partition",
            [](mofka::ServiceHandle& service,
               std::string_view topic_name,
               size_t server_rank,
               std::string_view metadata_provider,
               std::string_view data_provider,
               const nlohmann::json& partition_config,
               const std::string& pool_name) {
                service.addDefaultPartition(
                    topic_name, server_rank,
                    metadata_provider,
                    data_provider,
                    mofka::Metadata{partition_config},
                    pool_name);
            },
            "topic_name"_a, "server_rank"_a,
            "metadata_provider"_a=std::string_view{},
            "data_provider"_a=std::string_view{},
            "partition_config"_a=nlohmann::json::object(),
            "pool_name"_a="")
        .def("add_custom_partition",
            [](mofka::ServiceHandle& service,
               std::string_view topic_name,
               size_t server_rank,
               const std::string& partition_type,
               const nlohmann::json& partition_config,
               const mofka::ServiceHandle::PartitionDependencies& dependencies,
               const std::string& pool_name) {
                service.addCustomPartition(
                    topic_name, server_rank, partition_type,
                    mofka::Metadata{partition_config},
                    dependencies, pool_name);
            },
            "topic_name"_a, "server_rank"_a, "partition_type"_a="memory",
            "partition_config"_a=nlohmann::json::object(),
            "dependencies"_a=mofka::ServiceHandle::PartitionDependencies{},
            "pool_name"_a="")
        .def("add_memory_partition",
            [](mofka::ServiceHandle& service,
               std::string_view topic_name,
               size_t server_rank,
               const std::string& pool_name) {
                service.addMemoryPartition(topic_name, server_rank, pool_name);
            },
            "topic_name"_a, "server_rank"_a, "pool_name"_a="")
    ;

    py::class_<mofka::TopicHandle>(m, "TopicHandle")
        .def_property_readonly("name", &mofka::TopicHandle::name)
        .def_property_readonly("partitions", &mofka::TopicHandle::partitions)
        .def("producer",
            [](const mofka::TopicHandle& topic,
               std::string_view name,
               std::size_t batch_size,
               std::optional<mofka::ThreadPool> thread_pool,
               mofka::Ordering ordering) -> mofka::Producer {
                return topic.producer(
                    name, mofka::BatchSize(batch_size),
                    thread_pool.value_or(mofka::ThreadPool{mofka::ThreadCount{0}}),
                    ordering);
            },
            "name"_a="", "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "thread_pool"_a=std::optional<mofka::ThreadPool>{},
            "ordering"_a=mofka::Ordering::Strict)
        .def("consumer",
            [](const mofka::TopicHandle& topic,
               std::string_view name,
               std::size_t batch_size,
               std::optional<mofka::ThreadPool> thread_pool,
               PythonDataBroker broker,
               PythonDataSelector selector,
               std::optional<std::vector<size_t>> targets) -> mofka::Consumer {
                auto cpp_broker = broker ?
                    [broker=std::move(broker)]
                    (const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) -> mofka::Data {
                        auto segments = broker(metadata.json(), descriptor);
                        std::vector<mofka::Data::Segment> cpp_segments;
                        cpp_segments.reserve(segments.size());
                        for(auto& segment : segments) {
                            auto buf_info = get_buffer_info(segment.cast<py::buffer>());
                            check_buffer_is_writable(buf_info);
                            check_buffer_is_contiguous(buf_info);
                            cpp_segments.push_back({buf_info.ptr, (size_t)buf_info.size});
                        }
                        auto owner = new py::object{std::move(segments)};
                        auto free_cb = [owner](mofka::Data::Context) { delete owner; };
                        auto data = mofka::Data{std::move(cpp_segments), owner, std::move(free_cb)};
                        return data;
                }
                : mofka::DataBroker{};
                auto cpp_selector = selector ?
                    [selector=std::move(selector)]
                    (const mofka::Metadata& metadata, const mofka::DataDescriptor& descriptor) -> mofka::DataDescriptor {
                        std::optional<mofka::DataDescriptor> result = selector(metadata.json(), descriptor);
                        if(result) return result.value();
                        else return mofka::DataDescriptor::Null();
                    }
                : mofka::DataSelector{};
                std::vector<size_t> default_targets;
                return topic.consumer(
                    name, mofka::BatchSize(batch_size),
                    thread_pool.value_or(mofka::ThreadPool{mofka::ThreadCount{0}}),
                    mofka::DataBroker{cpp_broker},
                    mofka::DataSelector{cpp_selector},
                    targets.value_or(default_targets));
               },
            "name"_a, "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "thread_pool"_a=std::nullopt, "data_broker"_a=PythonDataBroker{},
            "data_selector"_a=PythonDataSelector{}, "targets"_a=std::optional<std::vector<size_t>>{})
    ;

    py::class_<mofka::Producer>(m, "Producer")
        .def_property_readonly("name", &mofka::Producer::name)
        .def_property_readonly("thread_pool", &mofka::Producer::threadPool)
        .def_property_readonly("topic", &mofka::Producer::topic)
        .def("push",
            [](const mofka::Producer& producer,
               std::string metadata,
               py::buffer b_data) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data));
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true))
        .def("push",
            [](const mofka::Producer& producer,
               nlohmann::json metadata,
               py::buffer b_data) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data));
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true))
        .def("push",
            [](const mofka::Producer& producer,
               std::string metadata,
               const py::list& b_data) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data));
            },
            "metadata"_a, "data"_a=std::vector<py::memoryview>{})
        .def("push",
            [](const mofka::Producer& producer,
               nlohmann::json metadata,
               py::list b_data) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data));
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true))
        .def("flush", &mofka::Producer::flush)
        .def("batch_size",
            [](const mofka::Producer& producer) -> std::size_t {
                return producer.batchSize().value;
            })
    ;

    py::class_<mofka::Consumer>(m, "Consumer")
        .def_property_readonly("name", &mofka::Consumer::name)
        .def_property_readonly("thread_pool", &mofka::Consumer::threadPool)
        .def_property_readonly("topic", &mofka::Consumer::topic)
        .def_property_readonly("data_broker", &mofka::Consumer::dataBroker)
        .def_property_readonly("data_selector", &mofka::Consumer::dataSelector)
        .def("batch_size",
            [](const mofka::Consumer& consumer) -> std::size_t {
                return consumer.batchSize().value;
            })
        .def("pull", &mofka::Consumer::pull)
        .def("process",
            [](const mofka::Consumer& consumer,
               mofka::EventProcessor processor,
               mofka::ThreadPool threadPool,
               std::size_t maxEvents) {
                return consumer.process(processor, threadPool, mofka::NumEvents{maxEvents});
               },
            "processor"_a, "threadPoll"_a,
            "max_events"_a=std::numeric_limits<size_t>::max()
            )
    ;

    py::class_<mofka::DataDescriptor>(m, "DataDescriptor")
        .def(py::init<>())
        .def(py::init(&mofka::DataDescriptor::From))
        .def_property_readonly("size", &mofka::DataDescriptor::size)
        .def_property_readonly("location",
            py::overload_cast<>(&mofka::DataDescriptor::location))
        .def_property_readonly("location",
            py::overload_cast<>(&mofka::DataDescriptor::location, py::const_))
        .def("make_stride_view",
            [](const mofka::DataDescriptor& data_descriptor,
               std::size_t offset,
               std::size_t numblocks,
               std::size_t blocksize,
               std::size_t gapsize) -> mofka::DataDescriptor {
                return data_descriptor.makeStridedView(offset, numblocks, blocksize, gapsize);
            },
            "offset"_a, "numblocks"_a, "blocksize"_a, "gapsize"_a)
        .def("make_sub_view",
            [](const mofka::DataDescriptor& data_descriptor,
               std::size_t offset,
               std::size_t size) -> mofka::DataDescriptor {
                return data_descriptor.makeSubView(offset, size);
            },
            "offset"_a, "size"_a)
        .def("make_unstructured_view",
            [](const mofka::DataDescriptor& data_descriptor,
               const std::vector<std::pair<std::size_t, std::size_t>> segments) -> mofka::DataDescriptor {
                return data_descriptor.makeUnstructuredView(segments);
            },
            "segments"_a)
    ;

    py::class_<mofka::Event>(m, "Event")
        .def_property_readonly("metadata",
                [](mofka::Event& event) { return event.metadata().string(); })
        .def_property_readonly("data",
                [](mofka::Event& event) {
                    auto owner = event.data().context();
                    return *static_cast<py::object*>(owner);
                })
        .def_property_readonly("event_id", &mofka::Event::id)
        .def_property_readonly("partition", &mofka::Event::partition)
        .def("acknowledge",
             [](const mofka::Event& event){
                event.acknowledge();
             })
    ;

    py::class_<mofka::Future<std::uint64_t>, std::shared_ptr<mofka::Future<std::uint64_t>>>(m, "FutureUint")
        .def("wait", &mofka::Future<std::uint64_t>::wait)
        .def_property_readonly("completed", &mofka::Future<std::uint64_t>::completed)
    ;

    py::class_<mofka::Future<mofka::Event>, std::shared_ptr<mofka::Future<mofka::Event>>>(m, "FutureEvent")
        .def("wait", &mofka::Future<mofka::Event>::wait)
        .def("completed", &mofka::Future<mofka::Event>::completed)
    ;
}
