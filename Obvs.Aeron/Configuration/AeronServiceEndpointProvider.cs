//using System;
//using System.Collections;
//using System.Collections.Generic;
//using System.Linq;
//using System.Reactive.Concurrency;
//using System.Reactive.Disposables;
//using System.Reflection;
//using Obvs.Configuration;
//using Obvs.Serialization;
//
//namespace Obvs.Aeron.Configuration
//{
//    public class AeronServiceEndpointProvider<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse> 
//        : IServiceEndpointProvider<TMessage, TCommand, TEvent, TRequest, TResponse>
//        where TMessage : class
//        where TServiceMessage : class
//        where TCommand : class, TMessage
//        where TEvent : class, TMessage
//        where TRequest : class, TMessage
//        where TResponse : class, TMessage
//    {
//        private readonly IMessageSerializer _serializer;
//        private readonly IMessageDeserializerFactory _deserializerFactory;
//        private readonly Func<Assembly, bool> _assemblyFilter;
//        private readonly Func<Type, bool> _typeFilter;
//        private readonly string _selector;
//        private readonly Func<IDictionary, bool> _propertyFilter;
//        private readonly Func<TMessage, Dictionary<string, object>> _propertyProvider;
//        private readonly Lazy<IConnection> _endpointConnection;
//        private readonly Lazy<IConnection> _endpointClientConnection;
//
//        public ActiveMQServiceEndpointProvider(string serviceName,
//            IMessageSerializer serializer,
//            IMessageDeserializerFactory deserializerFactory,
//            Func<Assembly, bool> assemblyFilter = null,
//            Func<Type, bool> typeFilter = null,
//            Lazy<IConnection> sharedConnection = null,
//            string selector = null,
//            Func<IDictionary, bool> propertyFilter = null,
//            Func<TMessage, Dictionary<string, object>> propertyProvider = null)
//        {
//            _serializer = serializer;
//            _deserializerFactory = deserializerFactory;
//            _assemblyFilter = assemblyFilter;
//            _typeFilter = typeFilter;
//            _selector = selector;
//            _propertyFilter = propertyFilter;
//            _propertyProvider = propertyProvider;
//
//            if (sharedConnection == null)
//            {
//                IConnectionFactory endpointConnectionFactory = new ConnectionFactory(brokerUri, ConnectionClientId.CreateWithSuffix(string.Format("{0}.Endpoint", serviceName)));
//                IConnectionFactory endpointClientConnectionFactory = new ConnectionFactory(brokerUri, ConnectionClientId.CreateWithSuffix(string.Format("{0}.EndpointClient", serviceName)));
//                _endpointConnection = endpointConnectionFactory.CreateLazyConnection(userName, password);
//                _endpointClientConnection = endpointClientConnectionFactory.CreateLazyConnection(userName, password);
//            }
//            else
//            {
//                _endpointConnection = sharedConnection;
//                _endpointClientConnection = sharedConnection;
//            }
//        }
//
//        public IServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> CreateEndpoint()
//        {
//            return new DisposingServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse>(
//                new ServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse>(
//                    CreateSource<TRequest>(_endpointConnection, RequestsDestination),
//                    CreateSource<TCommand>(_endpointConnection, CommandsDestination),
//                    CreatePublisher<TEvent>(_endpointConnection, EventsDestination),
//                    CreatePublisher<TResponse>(_endpointConnection, ResponsesDestination),
//                    typeof(TServiceMessage)),
//                GetConnectionDisposable(_endpointConnection));
//        }
//
//        private IMessageSource<T> CreateSource<T>(Lazy<IConnection> connection, string destination) where T : class, TMessage
//        {
//            List<Tuple<Type, AcknowledgementMode>> queueTypes;
//            var acknowledgementMode = GetAcknowledgementMode<T>();
//
//            if (TryGetMultipleQueueTypes<T>(out queueTypes))
//            {
//                var deserializers = _deserializerFactory.Create<T, TServiceMessage>(_assemblyFilter, _typeFilter).ToArray();
//                var topicSources = new[]
//                {
//                    new MessageSource<T>(connection, deserializers, new ActiveMQTopic(destination),
//                        acknowledgementMode, _selector, _propertyFilter)
//                };
//                var queueSources = queueTypes.Select(qt =>
//                    new MessageSource<T>(connection,
//                        deserializers,
//                        new ActiveMQQueue(GetTypedQueueName(destination, qt.Item1)),
//                        qt.Item2,
//                        _selector,
//                        _propertyFilter));
//
//                return new MergedMessageSource<T>(topicSources.Concat(queueSources));
//            }
//
//            return DestinationFactory.CreateSource<T, TServiceMessage>(connection, destination, GetDestinationType<T>(),
//                _deserializerFactory, _propertyFilter, _assemblyFilter, _typeFilter, _selector,
//                acknowledgementMode);
//        }
//
//        private IMessagePublisher<T> CreatePublisher<T>(Lazy<IConnection> connection, string destination) where T : class, TMessage
//        {
//            if (TryGetMultipleQueueTypes<T>(out queueTypes))
//            {
//                var topicTypes = MessageTypes.Get<T, TServiceMessage>().Where(type => queueTypes.All(qt => qt.Item1 != type));
//                var topicPublisher = new MessagePublisher<T>(connection, new ActiveMQTopic(destination), _serializer,
//                    _propertyProvider, Scheduler.Default);
//                var topicPublishers = topicTypes.Select(tt => new KeyValuePair<Type, IMessagePublisher<T>>(tt, topicPublisher));
//                var queuePubishers = queueTypes.Select(qt =>
//                    new KeyValuePair<Type, IMessagePublisher<T>>(qt.Item1,
//                        new MessagePublisher<T>(
//                            connection,
//                            new ActiveMQQueue(GetTypedQueueName(destination, qt.Item1)),
//                            _serializer,
//                            _propertyProvider,
//                            Scheduler.Default)));
//
//                return new TypeRoutingMessagePublisher<T>(topicPublishers.Concat(queuePubishers));
//            }
//
//            return DestinationFactory.CreatePublisher<T>(connection,
//                destination,
//                GetDestinationType<T>(),
//                _serializer,
//                _propertyProvider);
//        }
//
//        private static string GetTypedQueueName(string destination, Type type)
//        {
//            return string.Format("{0}.{1}", destination, type.Name);
//        }
//
//        public IServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse> CreateEndpointClient()
//        {
//            return new DisposingServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse>(
//                new ServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse>(
//                    CreateSource<TEvent>(_endpointClientConnection, EventsDestination),
//                    CreateSource<TResponse>(_endpointClientConnection, ResponsesDestination),
//                    CreatePublisher<TRequest>(_endpointClientConnection, RequestsDestination),
//                    CreatePublisher<TCommand>(_endpointClientConnection, CommandsDestination),
//                    typeof(TServiceMessage)),
//                GetConnectionDisposable(_endpointClientConnection));
//        }
//
//        private static IDisposable GetConnectionDisposable(Lazy<IConnection> lazyConnection)
//        {
//            return Disposable.Create(() =>
//            {
//                if (lazyConnection.IsValueCreated &&
//                    lazyConnection.Value.IsStarted)
//                {
//                    lazyConnection.Value.Stop();
//                    lazyConnection.Value.Close();
//                    lazyConnection.Value.Dispose();
//                }
//            });
//        }
//    }
//}