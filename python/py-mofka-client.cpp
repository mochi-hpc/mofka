#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <mofka/Client.hpp>
#include <mofka/ServiceHandle.hpp>
#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

typedef py::capsule py_margo_instance_id;
typedef py::capsule py_hg_addr_t;

#define MID2CAPSULE(__mid)   py::capsule((void*)(__mid),  "margo_instance_id")
#define ADDR2CAPSULE(__addr) py::capsule((void*)(__addr), "hg_addr_t")


PYBIND11_MODULE(pymofka_client, m) {
    m.doc() = "Python binding for the Mofka client library";

    py::class_<mofka::Client>(m, "Client")
        .def(py::init<py_margo_instance_id>(), "mid"_a)
        .def("connect",
             [](const mofka::Client& client, const std::string& ssgfile) -> mofka::ServiceHandle {
                return client.connect(mofka::SSGFileName{ssgfile});
             },
            "filename"_a)
        .def("connect",
             [](const mofka::Client& client, uint64_t ssgid) -> mofka::ServiceHandle {
                return client.connect(mofka::SSGGroupID{ssgid});
             },
            "gid"_a)
    ;

    py::class_<mofka::ServiceHandle>(m, "ServiceHandle")
        .def_property_readonly("num_servers", &mofka::ServiceHandle::numServers)
        .def("create_topic",
             [](mofka::ServiceHandle& service, const std::string& name) -> void {
                service.createTopic(name);
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
    ;
#if 0

        .def("make_database_handle",
             [](const mofka::Client& client,
                py_hg_addr_t addr,
                uint16_t provider_id,
                bool check) {
                return client.makeDatabaseHandle(addr, provider_id, check);
             },
             "address"_a, "provider_id"_a, "check"_a=true);

    py::class_<mofka::Database>(m, "Database")
        // --------------------------------------------------------------
        // COUNT
        // --------------------------------------------------------------
        .def("count",
             [](const mofka::Database& db, int32_t mode) {
                return db.count(mode);
             }, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT
        // --------------------------------------------------------------
        .def("put",
             static_cast<void(*)(const mofka::Database&, const py::buffer&,
                         const py::buffer&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("put",
             static_cast<void(*)(const mofka::Database&, const std::string&,
                         const py::buffer&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("put",
             static_cast<void(*)(const mofka::Database&, const std::string&,
                         const std::string&, int32_t)>(&put_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT_MULTI
        // --------------------------------------------------------------
        .def("put_multi",
             static_cast<void(*)(const mofka::Database&,
                const std::vector<std::pair<py::buffer,py::buffer>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("put_multi",
             static_cast<void(*)(const mofka::Database&,
                const std::vector<std::pair<std::string,py::buffer>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("put_multi",
             static_cast<void(*)(const mofka::Database&,
                const std::vector<std::pair<std::string,std::string>>&,
                int32_t)>(&put_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // PUT_PACKED
        // --------------------------------------------------------------
        .def("put_packed",
             [](const mofka::Database& db, const py::buffer& keys,
                const std::vector<size_t> key_sizes,
                const py::buffer& vals,
                const std::vector<size_t>& val_sizes,
                int32_t mode) {
                size_t count = key_sizes.size();
                if(count != val_sizes.size()) {
                    throw std::length_error("key_sizes and value_sizes should have the same length");
                }
                auto key_info = keys.request();
                auto val_info = vals.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                auto total_val_size = std::accumulate(val_sizes.begin(), val_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer is smaller than accumulated key_sizes");
                }
                if((ssize_t)total_val_size > val_info.itemsize*val_info.size) {
                    throw std::length_error("values buffer is smaller than accumulated value_sizes");
                }
                db.putPacked(count,
                       key_info.ptr,
                       key_sizes.data(),
                       val_info.ptr,
                       val_sizes.data(),
                       mode);
             }, "keys"_a, "key_sizes"_a, "values"_a, "value_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET
        // --------------------------------------------------------------
        .def("get",
             static_cast<size_t(*)(const mofka::Database&, const py::buffer&,
                py::buffer&, int32_t)>(&get_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("get",
             static_cast<size_t(*)(const mofka::Database&, const std::string&,
                py::buffer&, int32_t)>(&get_helper),
             "key"_a, "value"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET_MULTI
        // --------------------------------------------------------------
        .def("get_multi",
             static_cast<py::list(*)(const mofka::Database&,
                const std::vector<std::pair<py::buffer, py::buffer>>&,
                int32_t)>(&get_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("get_multi",
             static_cast<py::list(*)(const mofka::Database&,
                const std::vector<std::pair<std::string, py::buffer>>&,
                int32_t)>(&get_multi_helper),
             "pairs"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // GET_PACKED
        // --------------------------------------------------------------
        .def("get_packed",
             [](const mofka::Database& db, const py::buffer& keys,
                const std::vector<size_t>& key_sizes,
                py::buffer& vals,
                int32_t mode) {
                auto count = key_sizes.size();
                auto key_info = keys.request();
                auto val_info = vals.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                CHECK_BUFFER_IS_CONTIGUOUS(val_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                size_t vbuf_size = (size_t)(val_info.itemsize*val_info.size);
                std::vector<size_t> val_sizes(count);
                db.getPacked(count, key_info.ptr, key_sizes.data(),
                             vbuf_size, val_info.ptr, val_sizes.data(), mode);
                py::list result;
                for(size_t i = 0; i < count; i++) {
                    if(val_sizes[i] != YOKAN_KEY_NOT_FOUND)
                        result.append(val_sizes[i]);
                    else
                        result.append(py::none());
                }
                return result;
             }, "keys"_a, "key_sizes"_a, "values"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // FETCH
        // --------------------------------------------------------------
        .def("fetch",
             [](const mofka::Database& db, const py::buffer& key,
                std::function<void(size_t, const py::buffer&, const py::object&)> cb, int32_t mode) {
                auto key_info = get_buffer_info(key);
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto func =
                    [&cb](size_t index, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, py::memoryview::from_memory(key, ksize), py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, py::memoryview::from_memory(key, ksize), py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                db.fetch(
                    (const void*)key_info.ptr,
                    key_info.itemsize*key_info.size,
                    func, mode);
             },
             "key"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("fetch",
             [](const mofka::Database& db, const std::string& key,
                std::function<void(size_t, const std::string&, const py::object&)> cb, int32_t mode) {
                auto key_info = get_buffer_info(key);
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto func =
                    [&cb, &key](size_t index, const void*, size_t, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, key, py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, key, py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                db.fetch(
                    (const void*)key_info.ptr,
                    key_info.itemsize*key_info.size,
                    func, mode);
             },
             "key"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // FETCH_MULTI
        // --------------------------------------------------------------
        .def("fetch_multi",
             [](const mofka::Database& db, const std::vector<py::buffer>& keys,
                std::function<void(size_t, const py::buffer&, const py::object&)> cb,
                int32_t mode, unsigned batch_size) {
                std::vector<const void*> key_ptrs;
                std::vector<size_t>      key_sizes;
                key_ptrs.reserve(keys.size());
                key_sizes.reserve(keys.size());
                for(auto& key : keys) {
                    auto key_info = get_buffer_info(key);
                    CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                    key_ptrs.push_back(key_info.ptr);
                    key_sizes.push_back(key_info.itemsize*key_info.size);
                }
                auto func =
                    [&cb](size_t index, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, py::memoryview::from_memory(key, ksize), py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, py::memoryview::from_memory(key, ksize), py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                yk_fetch_options_t options;
                options.pool       = ABT_POOL_NULL;
                options.batch_size = batch_size;
                db.fetchMulti(
                    keys.size(), key_ptrs.data(), key_sizes.data(),
                    func, &options, mode);
             },
             "keys"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        .def("fetch_multi",
             [](const mofka::Database& db, const std::vector<std::string>& keys,
                std::function<void(size_t, const std::string&, const py::object&)> cb,
                int32_t mode, unsigned batch_size) {
                std::vector<const void*> key_ptrs;
                std::vector<size_t>      key_sizes;
                key_ptrs.reserve(keys.size());
                key_sizes.reserve(keys.size());
                for(auto& key : keys) {
                    key_ptrs.push_back(key.data());
                    key_sizes.push_back(key.size());
                }
                auto func =
                    [&cb, &keys](size_t index, const void*, size_t, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, keys[index], py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, keys[index], py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                yk_fetch_options_t options;
                options.pool       = ABT_POOL_NULL;
                options.batch_size = batch_size;
                db.fetchMulti(
                    keys.size(), key_ptrs.data(), key_sizes.data(),
                    func, &options, mode);
             },
             "keys"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        // --------------------------------------------------------------
        // FETCH_PACKED
        // --------------------------------------------------------------
        .def("fetch_packed",
             [](const mofka::Database& db, const py::buffer& keys,
                const std::vector<size_t>& ksizes,
                std::function<void(size_t, const py::buffer&, const py::object&)> cb,
                int32_t mode, unsigned batch_size) {
                auto key_info = get_buffer_info(keys);
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto total_key_size = std::accumulate(ksizes.begin(), ksizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                auto func =
                    [&cb](size_t index, const void* key, size_t ksize, const void* val, size_t vsize) -> yk_return_t {
                        try {
                            if(vsize <= YOKAN_LAST_VALID_SIZE)
                                cb(index, py::memoryview::from_memory(key, ksize), py::memoryview::from_memory(val, vsize));
                            else
                                cb(index, py::memoryview::from_memory(key, ksize), py::none());
                        } catch(py::error_already_set &e) {
                            return YOKAN_ERR_OTHER;
                        }
                        return YOKAN_SUCCESS;
                    };
                yk_fetch_options_t options;
                options.pool       = ABT_POOL_NULL;
                options.batch_size = batch_size;
                db.fetchPacked(ksizes.size(),
                    key_info.ptr, ksizes.data(),
                    func, &options, mode);
             },
             "keys"_a, "key_sizes"_a, "callback"_a, "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0)
        // --------------------------------------------------------------
        // EXISTS
        // --------------------------------------------------------------
        .def("exists",
             static_cast<bool(*)(const mofka::Database&, const std::string&, int32_t)>(&exists_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("exists",
             static_cast<bool(*)(const mofka::Database&, const py::buffer&, int32_t)>(&exists_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS_MULTI
        // --------------------------------------------------------------
        .def("exists_multi",
             static_cast<std::vector<bool>(*)(const mofka::Database&,
                 const std::vector<std::string>&, int32_t)>(&exists_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("exists_multi",
             static_cast<std::vector<bool>(*)(const mofka::Database&,
                 const std::vector<py::buffer>&, int32_t)>(&exists_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // EXISTS_PACKED
        // --------------------------------------------------------------
        .def("exists_packed",
             [](const mofka::Database& db, const py::buffer& keys,
                const std::vector<size_t>& key_sizes,
                int32_t mode) {
                auto count = key_sizes.size();
                auto key_info = keys.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                return db.existsPacked(count, key_info.ptr, key_sizes.data(), mode);
             }, "keys"_a, "key_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH
        // --------------------------------------------------------------
        .def("length",
             static_cast<size_t(*)(const mofka::Database&, const std::string&, int32_t)>(&length_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("length",
             static_cast<size_t(*)(const mofka::Database&, const py::buffer&, int32_t)>(&length_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH_MULTI
        // --------------------------------------------------------------
        .def("length_multi",
             static_cast<py::list(*)(const mofka::Database&,
                         const std::vector<std::string>&,
                         int32_t)>(&length_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("length_multi",
             static_cast<py::list(*)(const mofka::Database&,
                         const std::vector<py::buffer>&,
                         int32_t)>(&length_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LENGTH_PACKED
        // --------------------------------------------------------------
        .def("length_packed",
             [](const mofka::Database& db, const py::buffer& keys,
                const std::vector<size_t>& key_sizes,
                int32_t mode) {
                auto count = key_sizes.size();
                auto key_info = keys.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                std::vector<size_t> val_sizes(count);
                db.lengthPacked(count, key_info.ptr, key_sizes.data(), val_sizes.data(), mode);
                py::list result;
                for(size_t i = 0; i < count; i++) {
                    if(val_sizes[i] != YOKAN_KEY_NOT_FOUND)
                        result.append(val_sizes[i]);
                    else
                        result.append(py::none());
                }
                return result;
             }, "keys"_a, "key_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE
        // --------------------------------------------------------------
        .def("erase",
             static_cast<void(*)(const mofka::Database&, const std::string&, int32_t)>(&erase_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("erase",
             static_cast<void(*)(const mofka::Database&, const py::buffer&, int32_t)>(&erase_helper),
             "key"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE_MULTI
        // --------------------------------------------------------------
        .def("erase_multi",
             static_cast<void(*)(const mofka::Database&,
                                 const std::vector<std::string>&,
                                 int32_t)>(&erase_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("erase_multi",
             static_cast<void(*)(const mofka::Database&,
                                 const std::vector<py::buffer>&,
                                 int32_t)>(&erase_multi_helper),
             "keys"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ERASE_PACKED
        // --------------------------------------------------------------
        .def("erase_packed",
             [](const mofka::Database& db, const py::buffer& keys,
                const std::vector<size_t>& key_sizes,
                int32_t mode) {
                auto count = key_sizes.size();
                auto key_info = keys.request();
                CHECK_BUFFER_IS_CONTIGUOUS(key_info);
                auto total_key_size = std::accumulate(key_sizes.begin(), key_sizes.end(), (size_t)0);
                if((ssize_t)total_key_size > key_info.itemsize*key_info.size) {
                    throw std::length_error("keys buffer size smaller than accumulated key_sizes");
                }
                db.erasePacked(count, key_info.ptr, key_sizes.data(), mode);
             }, "keys"_a, "key_sizes"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYS
        // --------------------------------------------------------------
        .def("list_keys",
             static_cast<py::list(*)(const mofka::Database&,
                std::vector<py::buffer>&,
                const py::buffer&,
                const py::buffer&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const mofka::Database&,
                std::vector<py::buffer>&,
                const py::buffer&,
                const std::string&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const mofka::Database&,
                std::vector<py::buffer>&,
                const std::string&,
                const py::buffer&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys",
             static_cast<py::list(*)(const mofka::Database&,
                std::vector<py::buffer>&,
                const std::string&,
                const std::string&,
                int32_t)>(&list_keys_helper),
             "keys"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYS_PACKED
        // --------------------------------------------------------------
        .def("list_keys_packed",
             static_cast<py::list(*)(const mofka::Database&,
                py::buffer&, size_t, const py::buffer&,
                const py::buffer&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const mofka::Database&,
                py::buffer&, size_t, const std::string&,
                const py::buffer&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const mofka::Database&,
                py::buffer&, size_t, const py::buffer&,
                const std::string&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keys_packed",
             static_cast<py::list(*)(const mofka::Database&,
                py::buffer&, size_t, const std::string&,
                const std::string&, int32_t)>(&list_keys_packed_helper),
             "keys"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYVALS
        // --------------------------------------------------------------
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const mofka::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const py::buffer&, const py::buffer&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const mofka::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const std::string&, const py::buffer&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const mofka::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const py::buffer&, const std::string&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const mofka::Database&,
                std::vector<std::pair<py::buffer, py::buffer>>&,
                const std::string&, const std::string&,
                int32_t)>(&list_keyvals_helper),
             "pairs"_a, "from_key"_a,
             "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // LIST_KEYVALS_PACKED
        // --------------------------------------------------------------
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const mofka::Database&, py::buffer&, py::buffer&, size_t,
                const py::buffer&, const py::buffer&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const mofka::Database&, py::buffer&, py::buffer&, size_t,
                const std::string&, const py::buffer&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const mofka::Database&, py::buffer&, py::buffer&, size_t,
                const py::buffer&, const std::string&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("list_keyvals_packed",
             static_cast<std::vector<std::pair<ssize_t, ssize_t>>(*)(
                const mofka::Database&, py::buffer&, py::buffer&, size_t,
                const std::string&, const std::string&,
                int32_t)>(&list_keyvals_packed_helper),
             "keys"_a, "values"_a, "count"_a,
             "from_key"_a, "filter"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        // --------------------------------------------------------------
        // ITER
        // --------------------------------------------------------------
        .def("iter",
             static_cast<void(*)(
                const mofka::Database&,
                std::function<void(size_t, const py::buffer&, const py::object&)>,
                const py::buffer&, const py::buffer&,
                size_t, int32_t, unsigned, bool)>(&iter_helper),
             "callback"_a,
             "from_key"_a=py::bytes{}, "filter"_a=py::bytes{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0, "ignore_values"_a=false)
        .def("iter",
             static_cast<void(*)(
                const mofka::Database&,
                std::function<void(size_t, const py::buffer&, const py::object&)>,
                const py::buffer&, const std::string&,
                size_t, int32_t, unsigned, bool)>(&iter_helper),
             "callback"_a,
             "from_key"_a=py::bytes{}, "filter"_a=std::string{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0, "ignore_values"_a=false)
        .def("iter",
             static_cast<void(*)(
                const mofka::Database&,
                std::function<void(size_t, const std::string&, const py::object&)>,
                const std::string&, const py::buffer&,
                size_t, int32_t, unsigned, bool)>(&iter_helper),
             "callback"_a,
             "from_key"_a=std::string{}, "filter"_a=py::bytes{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0, "ignore_values"_a=false)
        .def("iter",
             static_cast<void(*)(
                const mofka::Database&,
                std::function<void(size_t, const std::string&, const py::object&)>,
                const std::string&, const std::string&,
                size_t, int32_t, unsigned, bool)>(&iter_helper),
             "callback"_a,
             "from_key"_a=std::string{}, "filter"_a=std::string{}, "count"_a=0,
             "mode"_a=YOKAN_MODE_DEFAULT, "batch_size"_a=0, "ignore_values"_a=false)
        // --------------------------------------------------------------
        // COLLECTION MANAGEMENT
        // --------------------------------------------------------------
        .def("create_collection",
             [](const mofka::Database& db, const std::string& name, int32_t mode) {
                db.createCollection(name.c_str(), mode);
                return mofka::Collection(name.c_str(), db);
             },
             "name"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("__getitem__",
             [](const mofka::Database& db, const std::string& name) {
                return mofka::Collection(name.c_str(), db);
             },
             "name"_a)
        .def("drop_collection",
             [](const mofka::Database& db, const std::string& name, int32_t mode) {
                db.dropCollection(name.c_str(), mode);
             },
             "name"_a, "mode"_a=YOKAN_MODE_DEFAULT)
        .def("collection_exists",
             [](const mofka::Database& db, const std::string& name, int32_t mode) {
                return db.collectionExists(name.c_str(), mode);
             },
             "name"_a, "mode"_a=YOKAN_MODE_DEFAULT)
    ;
#endif
}

