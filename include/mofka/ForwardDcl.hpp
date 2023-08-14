/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_FORWARD_DECL_HPP
#define MOFKA_FORWARD_DECL_HPP

namespace mofka {

class Archive;
struct BatchSize;
struct BufferWrapperOutputArchive;
struct BufferWrapperInputArchive;
struct BulkRef;
class Client;
class Consumer;
class ConsumerHandle;
class Data;
class DataDescriptor;
class Event;
struct StopEventProcessor;
class Exception;
template<typename ResultType> class Future;
class Metadata;
struct NumEvents;
class Producer;
class Provider;
template<typename T>
class RequestResult;
class SerializerInterface;
class Serializer;
class ServiceHandle;
struct SSGFileName;
struct SSGGroupID;
class PartitionTargetInfo;
class TargetSelectorInterface;
class TargetSelector;
struct ThreadCount;
class ThreadPool;
struct TopicBackendConfig;
class TopicHandle;
class TopicManager;
struct UUID;
class InvalidMetadata;
class ValidatorInterface;
class Validator;

}

#endif
