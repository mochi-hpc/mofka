#define PYBIND11_DETAILED_ERROR_MESSAGES
#define PYBIND11_NO_ASSERT_GIL_HELD_INCREF_DECREF

#include <mofka/MofkaDriver.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include "pybind11_json/pybind11_json.hpp"

#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

PYBIND11_MODULE(pymofka_client, m) {
    m.doc() = "Python binding for the MofkaDriver class";

    auto pydiaspora_stream_api = py::module_::import("pydiaspora_stream_api");
    py::object DriverInterface = (py::object)pydiaspora_stream_api.attr("Driver");

    py::class_<mofka::MofkaDriver,
               std::shared_ptr<mofka::MofkaDriver>>(m, "MofkaDriver", DriverInterface)
        .def(py::init([](const nlohmann::json& options) {
            return std::dynamic_pointer_cast<mofka::MofkaDriver>(
                mofka::MofkaDriver::create(diaspora::Metadata{options}));
        }))
        .def_property_readonly("num_servers", &mofka::MofkaDriver::numServers)
        .def("start_progress_thread", &mofka::MofkaDriver::startProgressThread)
        .def("add_legacy_partition",
            [](mofka::MofkaDriver& service,
               std::string_view topic_name,
               size_t server_rank,
               std::string_view metadata_provider,
               std::string_view data_provider,
               const nlohmann::json& partition_config,
               const std::string& pool_name) {
                service.addLegacyPartition(
                    topic_name, server_rank,
                    metadata_provider,
                    data_provider,
                    diaspora::Metadata{partition_config},
                    pool_name);
            },
            "topic_name"_a, "server_rank"_a,
            "metadata_provider"_a=std::string_view{},
            "data_provider"_a=std::string_view{},
            "partition_config"_a=nlohmann::json::object(),
            "pool_name"_a="")
        .def("add_yokan_metadata_provider",
            [](mofka::MofkaDriver& service,
               size_t server_rank,
               const std::string database_type,
               const nlohmann::json& database_config,
               const mofka::MofkaDriver::Dependencies& dependencies) {
                diaspora::Metadata config;
                config.json()["database"] = nlohmann::json::object();
                config.json()["database"]["type"] = database_type;
                config.json()["database"]["config"] = database_config;
                return service.addYokanMetadataProvider(server_rank, config, dependencies);
            },
            "server_rank"_a,
            "database_type"_a="map",
            "database_config"_a=nlohmann::json::object(),
            "dependencies"_a=mofka::MofkaDriver::Dependencies{})
        .def("add_yokan_metadata_provider",
            [](mofka::MofkaDriver& service,
               size_t server_rank,
               const nlohmann::json& config,
               const mofka::MofkaDriver::Dependencies& dependencies) {
                return service.addYokanMetadataProvider(
                    server_rank, diaspora::Metadata{config}, dependencies);
            },
            "server_rank"_a,
            "config"_a=R"({"database":{"type":"map","config":{}}})"_json,
            "dependencies"_a=mofka::MofkaDriver::Dependencies{})
        .def("add_warabi_data_provider",
            [](mofka::MofkaDriver& service,
               size_t server_rank,
               const std::string target_type,
               const nlohmann::json& target_config,
               const mofka::MofkaDriver::Dependencies& dependencies) {
                diaspora::Metadata config;
                config.json()["target"] = nlohmann::json::object();
                config.json()["target"]["type"] = target_type;
                config.json()["target"]["config"] = target_config;
                return service.addWarabiDataProvider(server_rank, config, dependencies);
            },
            "server_rank"_a, "target_type"_a="memory",
            "target_config"_a=nlohmann::json::object(),
            "dependencies"_a=mofka::MofkaDriver::Dependencies{})
        .def("add_warabi_data_provider",
            [](mofka::MofkaDriver& service,
               size_t server_rank,
               const nlohmann::json& config,
               const mofka::MofkaDriver::Dependencies& dependencies) {
                return service.addWarabiDataProvider(server_rank, diaspora::Metadata{config}, dependencies);
            },
            "server_rank"_a,
            "config"_a=R"({"target":{"type":"memory","config":{}}})"_json,
            "dependencies"_a=mofka::MofkaDriver::Dependencies{})
        .def("add_custom_partition",
            [](mofka::MofkaDriver& service,
               std::string_view topic_name,
               size_t server_rank,
               const std::string& partition_type,
               const nlohmann::json& partition_config,
               const mofka::MofkaDriver::Dependencies& dependencies,
               const std::string& pool_name) {
                service.addCustomPartition(
                    topic_name, server_rank, partition_type,
                    diaspora::Metadata{partition_config},
                    dependencies, pool_name);
            },
            "topic_name"_a, "server_rank"_a, "partition_type"_a="memory",
            "partition_config"_a=nlohmann::json::object(),
            "dependencies"_a=mofka::MofkaDriver::Dependencies{},
            "pool_name"_a="")
        .def("add_memory_partition",
            [](mofka::MofkaDriver& service,
               std::string_view topic_name,
               size_t server_rank,
               const std::string& pool_name) {
                service.addMemoryPartition(topic_name, server_rank, pool_name);
            },
            "topic_name"_a, "server_rank"_a, "pool_name"_a="")
    ;

#if 0
    py::class_<mofka::TopicHandle>(m, "TopicHandle")
        .def_property_readonly("name", &mofka::TopicHandle::name)
        .def_property_readonly("partitions", [](const mofka::TopicHandle& topic) {
            std::vector<nlohmann::json> result;
            for(auto& p : topic.partitions()) {
                result.push_back(p.json());
            }
            return result;
        })
        .def("mark_as_complete", &mofka::TopicHandle::markAsComplete)
        .def("producer",
            [](const mofka::TopicHandle& topic,
               std::string_view name,
               std::size_t batch_size,
               std::size_t max_batch,
               std::optional<mofka::ThreadPool> thread_pool,
               mofka::Ordering ordering) -> mofka::Producer {
                if(!thread_pool.has_value())
                    thread_pool = mofka::ThreadPool{mofka::ThreadCount{0}};
                return topic.producer(
                    name, mofka::BatchSize(batch_size),
                    mofka::MaxBatch{max_batch}, thread_pool.value(),
                    ordering);
            },
            "name"_a="", py::kw_only(),
            "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "max_batch"_a=2, "thread_pool"_a=std::optional<mofka::ThreadPool>{},
            "ordering"_a=mofka::Ordering::Strict)
        .def("consumer",
            [](const mofka::TopicHandle& topic,
               std::string_view name,
               PythonDataSelector selector,
               PythonDataBroker broker,
               std::size_t batch_size,
               std::size_t max_batch,
               std::optional<mofka::ThreadPool> thread_pool,
               std::optional<std::vector<size_t>> targets) -> mofka::Consumer {
                auto cpp_broker = broker ?
                    [broker=std::move(broker)]
                    (const mofka::Metadata& metadata,
                     const mofka::DataDescriptor& descriptor) -> mofka::Data {
                        auto segments = broker(metadata.json(), descriptor);
                        std::vector<mofka::Data::Segment> cpp_segments;
                        cpp_segments.reserve(segments.size());
                        for(auto& segment : segments) {
                            auto buf_info = get_buffer_info(segment.cast<py::buffer>());
                            check_buffer_is_writable(buf_info);
                            check_buffer_is_contiguous(buf_info);
                            cpp_segments.push_back({buf_info.ptr, (size_t)buf_info.size});
                        }
                        auto owner = new PythonDataOwner{std::move(segments)};
                        auto free_cb = [owner](mofka::Data::Context) { delete owner; };
                        auto data = mofka::Data{std::move(cpp_segments), owner, std::move(free_cb)};
                        return data;
                }
                : mofka::DataBroker{};
                auto cpp_selector = selector ?
                    [selector=std::move(selector)]
                    (const mofka::Metadata& metadata,
                     const mofka::DataDescriptor& descriptor) -> mofka::DataDescriptor {
                        std::optional<mofka::DataDescriptor> result = selector(metadata.json(), descriptor);
                        if(result) return result.value();
                        else return mofka::DataDescriptor::Null();
                    }
                : mofka::DataSelector{};
                std::vector<size_t> default_targets;
                if(!thread_pool.has_value())
                    thread_pool = mofka::ThreadPool{mofka::ThreadCount{0}};
                return topic.consumer(
                    name, mofka::BatchSize(batch_size),
                    mofka::MaxBatch{max_batch},
                    thread_pool.value(),
                    mofka::DataBroker{cpp_broker},
                    mofka::DataSelector{cpp_selector},
                    targets.value_or(default_targets));
               },
            "name"_a, py::kw_only(),
            "data_selector"_a, "data_broker"_a,
            "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "max_batch"_a=2, "thread_pool"_a=std::nullopt,
            "targets"_a=std::optional<std::vector<size_t>>{})
        .def("consumer",
            [](const mofka::TopicHandle& topic,
               std::string_view name,
               std::size_t batch_size,
               std::size_t max_batch,
               std::optional<mofka::ThreadPool> thread_pool,
               std::optional<std::vector<size_t>> targets) -> mofka::Consumer {
                auto cpp_broker = [](const mofka::Metadata& metadata,
                                     const mofka::DataDescriptor& descriptor) -> mofka::Data {
                        (void)metadata;
                        auto owner = new BufferDataOwner{descriptor.size()};
                        std::vector<mofka::Data::Segment> cpp_segment{
                            mofka::Data::Segment{owner->m_data.data(), owner->m_data.size()}
                        };
                        auto free_cb = [owner](mofka::Data::Context) { delete owner; };
                        auto data = mofka::Data{std::move(cpp_segment), owner, std::move(free_cb)};
                        return data;
                };
                auto cpp_selector = [](const mofka::Metadata& metadata,
                                       const mofka::DataDescriptor& descriptor) -> mofka::DataDescriptor {
                        (void)metadata;
                        return descriptor;
                };
                std::vector<size_t> default_targets;
                if(!thread_pool.has_value())
                    thread_pool = mofka::ThreadPool{mofka::ThreadCount{0}};
                return topic.consumer(
                    name, mofka::BatchSize(batch_size),
                    mofka::MaxBatch{max_batch}, thread_pool.value(),
                    mofka::DataBroker{cpp_broker},
                    mofka::DataSelector{cpp_selector},
                    targets.value_or(default_targets));
               },
            "name"_a, py::kw_only(),
            "batch_size"_a=mofka::BatchSize::Adaptive().value,
            "max_batch"_a=2, "thread_pool"_a=std::nullopt,
            "targets"_a=std::optional<std::vector<size_t>>{})
    ;

    py::class_<mofka::Producer>(m, "Producer")
        .def_property_readonly("name", &mofka::Producer::name)
        .def_property_readonly("thread_pool", &mofka::Producer::threadPool)
        .def_property_readonly("topic", &mofka::Producer::topic)
        .def("push",
            [](const mofka::Producer& producer,
               std::string metadata,
               py::buffer b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](const mofka::Producer& producer,
               nlohmann::json metadata,
               py::buffer b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](const mofka::Producer& producer,
               std::string metadata,
               const py::list& b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            },
            "metadata"_a, "data"_a=std::vector<py::memoryview>{},
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](const mofka::Producer& producer,
               nlohmann::json metadata,
               py::list b_data,
               std::optional<size_t> part) -> mofka::Future<mofka::EventID> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            },
            "metadata"_a, "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
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
            "processor"_a, py::kw_only(), "thread_pool"_a,
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
                    auto owner = static_cast<AbstractDataOwner*>(event.data().context());
                    return owner->toPythonObject();
                    //return *static_cast<py::object*>(owner);
                })
        .def_property_readonly("event_id", [](const mofka::Event& event) -> py::object {
                if(event.id() == mofka::NoMoreEvents)
                    return py::none();
                else
                    return py::cast(event.id());
                })
        .def_property_readonly("partition",
                [](const mofka::Event& event) {
                    return event.partition().json();
                })
        .def("acknowledge",
             [](const mofka::Event& event){
                event.acknowledge();
             })
    ;

    py::class_<mofka::Future<std::uint64_t>, std::shared_ptr<mofka::Future<std::uint64_t>>>(m, "FutureUint")
        .def("wait", [](mofka::Future<std::uint64_t>& future) {
            std::uint64_t result;
            Py_BEGIN_ALLOW_THREADS
            result = future.wait();
            Py_END_ALLOW_THREADS
            return result;
        })
        .def_property_readonly("completed", &mofka::Future<std::uint64_t>::completed)
    ;

    py::class_<mofka::Future<mofka::Event>, std::shared_ptr<mofka::Future<mofka::Event>>>(m, "FutureEvent")
        .def("wait", [](mofka::Future<mofka::Event>& future) {
                mofka::Event result;
            Py_BEGIN_ALLOW_THREADS
            result = future.wait();
            Py_END_ALLOW_THREADS
            return result;
        })
        .def("completed", &mofka::Future<mofka::Event>::completed)
    ;

    PythonDataSelector select_full_data =
        [](const nlohmann::json&, const mofka::DataDescriptor& d) -> std::optional<mofka::DataDescriptor> {
            return d;
        };
    m.attr("FullDataSelector") = py::cast(select_full_data);

    PythonDataBroker bytes_data_broker =
        [](const nlohmann::json&, const mofka::DataDescriptor& d) -> py::list {
            auto buffer = py::bytearray();
            auto ret = PyByteArray_Resize(buffer.ptr(), d.size());
            py::list result;
            result.append(std::move(buffer));
            return result;
        };
    m.attr("ByteArrayAllocator") = py::cast(bytes_data_broker);
#endif
}
