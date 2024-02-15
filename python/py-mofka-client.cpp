#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <mofka/Client.hpp>
#include <mofka/ServiceHandle.hpp>
#include <mofka/TopicHandle.hpp>
#include <mofka/ThreadPool.hpp>


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

static auto get_buffer_info(const std::string& str) {
    return py::buffer_info{ str.data(), (ssize_t)str.size(), false };
}

#define CHECK_BUFFER_IS_CONTIGUOUS(__buf_info__) do { \
    ssize_t __stride__ = (__buf_info__).itemsize;     \
    for(ssize_t i=0; i < (__buf_info__).ndim; i++) {  \
        if(__stride__ != (__buf_info__).strides[i])   \
            throw mofka::Exception("MOFKA_ERR_NONCONTIG");  \
        __stride__ *= (__buf_info__).shape[i];        \
    }                                                 \
} while(0)

#define CHECK_BUFFER_IS_WRITABLE(__buf_info__) do { \
    if((__buf_info__).readonly)                     \
        throw mofka::Exception("MOFKA_ERR_READONLY");     \
} while(0)


template <typename DataType>
auto data_helper(const DataType& data){
    auto data_info = get_buffer_info(data);
    CHECK_BUFFER_IS_CONTIGUOUS(data_info);
    return mofka::Data(data_info.ptr, data_info.size);
}

template<typename DataType>
static auto data_helper(const std::vector<DataType>& buffers) {
    std::vector<mofka::Data::Segment> segments;
    for (auto buff : buffers){
        auto buff_info = get_buffer_info(buff);
        CHECK_BUFFER_IS_CONTIGUOUS(buff_info);
        CHECK_BUFFER_IS_WRITABLE(buff_info);
        segments.push_back(mofka::Data::Segment(buff_info.ptr, 
                                                buff_info.size));
    }
    return mofka::Data(segments);
}

static auto metadata_helper(const py::dict metadata) {
    py::module_ json = py::module_::import("json");
    std::string str_metadata = json.attr("dumps")(metadata).cast<std::string>();
    return mofka::Metadata(str_metadata);
}

static auto data_broker_helper(const std::function<py::buffer(py::dict, mofka::DataDescriptor descriptor)> &broker,
                                 const mofka::Metadata metadata = mofka::Metadata{"{\"type\":\"default\"}"}, 
                                 const mofka::DataDescriptor data_descriptor = mofka::DataDescriptor()) {
    return [broker](mofka::Metadata metadata, 
                     mofka::DataDescriptor data_descriptor) -> mofka::Data {
        py::module_ json = py::module_::import("json");
        py::dict d_metadata = json.attr("loads")(metadata.string());
        py::buffer data_buffer = broker(d_metadata, data_descriptor).cast<py::buffer>();
        return data_helper(data_buffer);
    };
}

static auto data_selector_helper(const std::function<mofka::DataDescriptor(py::dict, mofka::DataDescriptor descriptor)> &selector,
                                 const mofka::Metadata metadata = mofka::Metadata{"{\"type\":\"default\"}"}, 
                                 const mofka::DataDescriptor data_descriptor = mofka::DataDescriptor()) {
    return  [selector](mofka::Metadata metadata, 
                       mofka::DataDescriptor data_descriptor) -> mofka::DataDescriptor {
        py::module_ json = py::module_::import("json");
        py::dict d_metadata = json.attr("loads")(metadata.string());
        return selector(d_metadata, data_descriptor);
    };

}
           

std::string stringify(const rapidjson::Value& v) {
	if (v.IsString())
		return { v.GetString(), v.GetStringLength() };
	else {
        rapidjson::StringBuffer strbuf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
        v.Accept(writer);
        return { strbuf.GetString(), strbuf.GetLength() };
	}
}

PYBIND11_MODULE(pymofka_client, m) {
    m.doc() = "Python binding for the Mofka client library";

    py::class_<mofka::Client>(m, "Client")
        .def(py::init<py_margo_instance_id>(), "mid"_a)
        .def("connect",
             [](const mofka::Client& client, const std::string_view ssgfile) -> mofka::ServiceHandle {
                return client.connect(mofka::SSGFileName{ssgfile});
             },
            "ssgfile"_a)
        .def("connect",
             [](const mofka::Client& client, uint64_t ssgid) -> mofka::ServiceHandle {
                return client.connect(mofka::SSGGroupID{ssgid});
             },
            "gid"_a)
        .def("get_config",
            [](const mofka::Client& client) -> const std::string {
                auto&  config = client.getConfig();
                return stringify(config);
            })
        .def_property_readonly("engine", &mofka::Client::engine)
    ;

    py::class_<mofka::Validator>(m, "Validator")
        .def(py::init<>())
        .def(py::init(&mofka::Validator::FromMetadata))
        .def(py::init(
            [](const py::dict d_metadata){
                mofka::Metadata metadata = metadata_helper(d_metadata);
                return mofka::Validator::FromMetadata(metadata);
            }))
        .def("validate",
            [](const mofka::Validator& validator, 
               const py::dict& d_metadata, 
               const py::buffer& b_data){
                mofka::Metadata metadata = metadata_helper(d_metadata);
                mofka::Data data = data_helper(b_data);            
                return validator.validate(metadata, data);
               },
            "metadata"_a, "data"_a)
        .def("validate",
            [](const mofka::Validator& validator, 
               const mofka::Metadata& metadata, 
               const mofka::Data& data){
                return validator.validate(metadata, data);
               },
            "metadata"_a, "data"_a)
        .def_property_readonly("metadata", &mofka::Validator::metadata)
    ;

    py::class_<mofka::ThreadPool>(m, "ThreadPool")
        .def(py::init(
            [](std::size_t count){
            return new mofka::ThreadPool(mofka::ThreadCount{count});
        }))
        .def("thread_count",
            [](const mofka::ThreadPool& thread_pool) -> std::size_t {
                mofka::ThreadCount t_count = thread_pool.threadCount();
                return t_count.count;
            })
    ;

    py::enum_<mofka::Ordering>(m, "Ordering")
        .value("Strict", mofka::Ordering::Strict)
        .value("Loose", mofka::Ordering::Loose)    
    ;

    py::class_<mofka::Serializer>(m, "Serializer")
        .def(py::init<>())
        .def(py::init(&mofka::Serializer::FromMetadata))
        .def(py::init(
            [](const py::dict d_metadata){
                mofka::Metadata metadata = metadata_helper(d_metadata);
                return mofka::Serializer::FromMetadata(metadata);
            }))
        .def("serialize",
            [](const mofka::Serializer& serializer, 
               mofka::Archive& archive, 
               const py::dict d_metadata){
                mofka::Metadata metadata = metadata_helper(d_metadata);
                return serializer.serialize(archive, metadata);
               },
            "archive"_a, "matadata"_a)
        .def("serialize",
            [](const mofka::Serializer& serializer, 
               mofka::Archive& archive, 
               const mofka::Metadata& metadata){
                return serializer.serialize(archive, metadata);
               },
            "archive"_a, "matadata"_a)
        .def("deserialize",
            [](const mofka::Serializer& deserializer, 
               mofka::Archive& archive, 
               py::dict d_metadata){
                mofka::Metadata metadata = metadata_helper(d_metadata);
                return deserializer.deserialize(archive, metadata);
               },
            "archive"_a, "matadata"_a)
        .def("deserialize",
            [](const mofka::Serializer& deserializer, 
               mofka::Archive& archive, 
               mofka::Metadata& metadata){
                return deserializer.deserialize(archive, metadata);
               },
            "archive"_a, "matadata"_a)
        .def_property_readonly("metadata", &mofka::Serializer::metadata)
    ;

    py::class_<mofka::PartitionInfo>(m, "PartitionInfo")
        .def_property_readonly("uuid", &mofka::PartitionInfo::uuid)
        .def_property_readonly("address", &mofka::PartitionInfo::address)
        .def_property_readonly("provider_id", &mofka::PartitionInfo::providerID)
    ;

    py::class_<mofka::PartitionSelector>(m, "PartitionSelector")
        .def(py::init<>())
        .def(py::init(&mofka::PartitionSelector::FromMetadata))
        .def(py::init(
            [](const py::dict d_metadata){
                mofka::Metadata metadata = metadata_helper(d_metadata);
                return mofka::PartitionSelector::FromMetadata(metadata);
            }))
        .def("set_partitions", 
            [](mofka::PartitionSelector& selector, const std::vector<mofka::PartitionInfo>& targets) {
                return selector.setPartitions(targets);                
            },
            "targets"_a)
        .def("select_partition_for",
            [](mofka::PartitionSelector& selector, 
               const mofka::Metadata& metadata) -> mofka::PartitionInfo {
                return selector.selectPartitionFor(metadata);
               },
            "metadata"_a)
        .def_property_readonly("metadata", &mofka::PartitionSelector::metadata)
    ;

    py::class_<mofka::ServiceHandle>(m, "ServiceHandle")
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
        .def("add_partition",
            [](mofka::ServiceHandle& service,
               std::string_view topic_name,
               size_t server_rank,
               const std::string& partition_type,
               const std::string& partition_config,
               const mofka::ServiceHandle::PartitionDependencies& dependencies,
               const std::string& pool_name) {
                service.addPartition(
                    topic_name, server_rank, partition_type,
                    mofka::Metadata{partition_config},
                    dependencies, pool_name);
            },
            "topic_name"_a, "server_rank"_a, "partition_type"_a="memory",
            "partition_config"_a="{}", "dependencies"_a=mofka::ServiceHandle::PartitionDependencies{},
            "pool_name"_a="")
        .def_property_readonly("client", &mofka::ServiceHandle::client)
    ;

    py::class_<mofka::TopicHandle>(m, "TopicHandle")
        .def("producer",
            [](const mofka::TopicHandle& topic,
               std::string_view name) -> mofka::Producer {
                return topic.producer(
                    name);
            },
            "name"_a)
        .def("producer",
            [](const mofka::TopicHandle& topic,
               std::string_view name,
               std::size_t batch_size,
               mofka::ThreadPool thread_pool,
               std::string s_ordering) -> mofka::Producer {
                mofka::Ordering ordering = (s_ordering == "Loose") ? mofka::Ordering::Loose : mofka::Ordering::Strict;
                return topic.producer(
                    name, mofka::BatchSize(batch_size), thread_pool, ordering);
            },
            "name"_a, "batch_size"_a, "thread_pool"_a, "ordering"_a)

        .def("consumer",
            [](const mofka::TopicHandle& topic,
               std::string_view name,
               std::size_t batch_size,
               mofka::ThreadPool thread_pool,
               std::function<mofka::Data(const mofka::Metadata&, const mofka::DataDescriptor&)> data_broker,
               std::function<mofka::DataDescriptor(const mofka::Metadata&, const mofka::DataDescriptor&)> data_selector,
               const std::vector<mofka::PartitionInfo>& targets) -> mofka::Consumer {
                return topic.consumer(
                    name, mofka::BatchSize(batch_size), thread_pool, data_broker, 
                    data_selector, targets);
               },
            "name"_a, "batch_size"_a, "thread_pool"_a, "data_broker"_a,
            "data_selector"_a, "targets"_a)

        .def("consumer",
            [](const mofka::TopicHandle& topic,
               std::string_view name,
               std::size_t batch_size,
               mofka::ThreadPool thread_pool,
               std::function<py::buffer(const py::dict, const mofka::DataDescriptor&)> broker,
               std::function<mofka::DataDescriptor(const py::dict, const mofka::DataDescriptor&)> selector,
               const std::vector<mofka::PartitionInfo>& targets) -> mofka::Consumer {
                auto data_broker = data_broker_helper(broker);
                auto data_selector = data_selector_helper(selector);
                return topic.consumer(
                    name, mofka::BatchSize(batch_size), thread_pool, data_broker, 
                    data_selector, targets);
               },
            "name"_a, "batch_size"_a, "thread_pool"_a, "data_broker"_a,
            "data_selector"_a, "targets"_a)
            
        .def("consumer",
            [](const mofka::TopicHandle& topic,
               std::string_view name) -> mofka::Consumer {
                return topic.consumer(
                    name);
               },
            "name"_a)
        .def_property_readonly("name", &mofka::TopicHandle::name)
        .def_property_readonly("service", &mofka::TopicHandle::service)
        .def_property_readonly("partitions", &mofka::TopicHandle::partitions)
    ;

    py::class_<mofka::Producer>(m, "Producer")
        .def("push",
            [](const mofka::Producer& producer, 
               mofka::Metadata metadata, 
               mofka::Data data) -> mofka::Future<std::uint64_t> {
                return producer.push(metadata, data);
            },
            "metadata"_a, "data"_a="{}")
        .def("push",
            [](const mofka::Producer& producer, 
               py::dict metadata, 
               py::buffer data) -> mofka::Future<std::uint64_t> {
                return producer.push(metadata_helper(metadata), data_helper(data));
            },
            "metadata"_a, "data"_a)
        .def("flush",
            [](mofka::Producer& producer){
                producer.flush();
            })
        .def_property_readonly("name", &mofka::Producer::name)
        .def("batchsize",
            [](const mofka::Producer& producer) -> std::size_t {
                mofka::BatchSize b_size = producer.batchSize();
                return b_size.value;
            })
        .def_property_readonly("threadPool", &mofka::Producer::threadPool)
        .def_property_readonly("topic", &mofka::Producer::topic)
    ;

    py::class_<mofka::Consumer>(m, "Consumer")
        .def_property_readonly("name", &mofka::Consumer::name)
        .def("batchsize",
            [](const mofka::Consumer& consumer) -> std::size_t {
                mofka::BatchSize b_size = consumer.batchSize();
                return b_size.value;
            })
        .def_property_readonly("thread_pool", &mofka::Consumer::threadPool)
        .def_property_readonly("topic", &mofka::Consumer::topic)
        .def_property_readonly("data_broker", &mofka::Consumer::dataBroker)
        .def_property_readonly("data_selector", &mofka::Consumer::dataSelector)
        .def("pull",
            [](mofka::Consumer& consumer) -> mofka::Future<mofka::Event> {
                return consumer.pull();
            })
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

    py::class_<mofka::Data::Segment>(m, "Segment")
        .def_readwrite("ptr", &mofka::Data::Segment::ptr)
        .def_readwrite("size", &mofka::Data::Segment::size)
    ;

    py::class_<mofka::Data>(m, "Data")
        .def(py::init<>())
        .def(py::init([](const py::buffer buffer){
            auto buffer_info = get_buffer_info(buffer);
            return new mofka::Data(buffer_info.ptr, buffer_info.size);
        }))
        .def(py::init([](std::vector<py::buffer> buffers){
            std::vector<mofka::Data::Segment> segments(buffers.size());
            for(size_t i = 0; i < buffers.size(); i++) {
                auto seg_info = get_buffer_info(buffers[i]);
                CHECK_BUFFER_IS_CONTIGUOUS(seg_info);
                segments[i] = mofka::Data::Segment{seg_info.ptr, (std::size_t)seg_info.size};
            }
            return new mofka::Data(segments);
        }))
        .def_property_readonly("segments", &mofka::Data::segments)
        .def_property_readonly("size", &mofka::Data::size)
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
               const std::map<std::size_t, std::size_t> segments) -> mofka::DataDescriptor {
                return data_descriptor.makeUnstructuredView(segments);
            },
            "segments"_a)
        .def("load", 
            [](mofka::DataDescriptor& data_descriptor,
               mofka::Archive& archive){
                return data_descriptor.load(archive);
            },
            "archive"_a)
        .def("save",
            [](const mofka::DataDescriptor& data_descriptor,
               mofka::Archive& archive){
                return data_descriptor.save(archive);
               },
            "archive"_a)
    ;

    py::class_<mofka::Metadata>(m, "Metadata")
        .def(py::init<std::string, bool>(), "json"_a="{}", "validate"_a=false)
        .def("__str__",
            [](mofka::Metadata& metadata){
                return metadata.string();
            })
        .def("is_valid_json",
            [](mofka::Metadata& metadata) -> bool {
                return metadata.isValidJson();
            })
    ;

    py::class_<mofka::Event>(m, "Event")
        .def_property_readonly("metadata", &mofka::Event::metadata)
        .def_property_readonly("data", &mofka::Event::data)
        .def_property_readonly("partition", &mofka::Event::partition)
        .def("acknowledge",
             [](const mofka::Event& event){
                event.acknowledge();
             })
    ;

    py::class_<mofka::Future<std::uint64_t>, std::shared_ptr<mofka::Future<std::uint64_t>>>(m, "FutureUint")
        .def("wait", &mofka::Future<std::uint64_t>::wait)
        .def("completed", &mofka::Future<std::uint64_t>::completed)
    ;

    py::class_<mofka::Future<mofka::Event>, std::shared_ptr<mofka::Future<mofka::Event>>>(m, "FutureEvent")
        .def("wait", &mofka::Future<mofka::Event>::wait)
        .def("completed", &mofka::Future<mofka::Event>::completed)
    ;
}