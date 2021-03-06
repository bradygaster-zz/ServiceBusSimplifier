﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusSimplifier
{
    public class ServiceBus
    {
        string _namespace;
        string _issuer;
        string _issuerKey;
        NamespaceManager _namespaceManager;
        MessagingFactory _messagingFactory;
        TokenProvider _tokenProvider;
        Uri _serviceUri;
        List<Tuple<string, SubscriptionClient>> _subscribers;
        private ReceiveMode _receiveMode;

        private ServiceBus()
        {
            _subscribers = new List<Tuple<string, SubscriptionClient>>();
        }

        private void SetupServiceBusEnvironment()
        {
            if (_namespaceManager == null)
            {
                _tokenProvider = TokenProvider.CreateSharedSecretTokenProvider(_issuer, _issuerKey);
                _serviceUri = ServiceBusEnvironment.CreateServiceUri("sb", _namespace, string.Empty);
                _messagingFactory = MessagingFactory.Create(_serviceUri, _tokenProvider);
                _namespaceManager = new NamespaceManager(_serviceUri, _tokenProvider);
            }
        }

        public static ServiceBus Setup(InitializationRequest request)
        {
            var ret = new ServiceBus
            {
                _namespace = request.Namespace,
                _issuer = request.Issuer,
                _issuerKey = request.IssuerKey
            };

            return ret;
        }

        public ServiceBus Subscribe<T>(Action<T> receiveHandler, ReceiveMode receiveMode = ReceiveMode.ReceiveAndDelete)
        {
            _receiveMode = receiveMode;
            SetupServiceBusEnvironment();
            var topicName = string.Format("Topic_{0}", typeof(T).Name);
            var subscriptionName = string.Format("Subscription_{0}", typeof(T).Name);

            if (!_namespaceManager.TopicExists(topicName))
                _namespaceManager.CreateTopic(topicName);

            var topic = _namespaceManager.GetTopic(topicName);

            SubscriptionDescription subscription;

            if (!_namespaceManager.SubscriptionExists(topic.Path, subscriptionName))
                subscription = _namespaceManager.CreateSubscription(topic.Path, subscriptionName);
            else
                subscription = _namespaceManager.GetSubscription(topic.Path, subscriptionName);

            var subscriptionClient = _messagingFactory.CreateSubscriptionClient(topicName, subscriptionName, receiveMode);

            _subscribers.Add(new Tuple<string, SubscriptionClient>(topicName, subscriptionClient));

            Begin<T>(receiveHandler, subscriptionClient);

            return this;
        }

        private void Begin<T>(Action<T> receiveHandler, SubscriptionClient subscriptionClient)
        {
            Debug.WriteLine("Calling BeginReceive");
            subscriptionClient.BeginReceive(
                TimeSpan.FromMinutes(5),
                (cb) => ProcessBrokeredMessage(receiveHandler, subscriptionClient, cb),
                null);
        }

        private void ProcessBrokeredMessage<T>(Action<T> receiveHandler, SubscriptionClient subscriptionClient, IAsyncResult cb)
        {
            try
            {
                var brokeredMessage = subscriptionClient.EndReceive(cb);
                if (brokeredMessage != null)
                {
                    var messageData = brokeredMessage.GetBody<T>();
                    try
                    {
                        receiveHandler(messageData);

                        if (_receiveMode == ReceiveMode.PeekLock)
                        {
                            brokeredMessage.BeginComplete((result) =>
                                                              {
                                                                  var m = result.AsyncState as BrokeredMessage;
                                                                  if (m != null) m.EndComplete(result);
                                                              }, brokeredMessage);
                        }
                    }
                    catch (Exception)
                    {
                        // TODO: what happens if this loops?
                        if (_receiveMode == ReceiveMode.PeekLock)
                        {
                            brokeredMessage.BeginAbandon((result) =>
                                                             {
                                                                 var m = result.AsyncState as BrokeredMessage;
                                                                 if (m != null) m.EndAbandon(result);
                                                             }, brokeredMessage);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine("BeginReceive threw exception");
                if (OnError != null)
                    OnError(ex); // report the error if the user wants to know the error
            }
            finally
            {
                Begin<T>(receiveHandler, subscriptionClient);
            }
        }

        public ServiceBus Publish<T>(T message)
        {
            SetupServiceBusEnvironment();
            var topicName = string.Format("Topic_{0}", typeof(T).Name);
            var topicClient = _messagingFactory.CreateTopicClient(topicName);

            try
            {
                topicClient.Send(new BrokeredMessage(message));
            }
            catch (Exception x)
            {
                throw x;
            }
            finally
            {
                topicClient.Close();
            }

            return this;
        }

        public ServiceBus Close()
        {
            _subscribers.ForEach((s) => s.Item2.Close());
            return this;
        }

        public ServiceBus ClearTopics()
        {
            _subscribers.ForEach((s) => _namespaceManager.DeleteTopic(s.Item1));
            return this;
        }

        public delegate void ServiceBusExceptionHandler(Exception exception);

        public event ServiceBusExceptionHandler OnError;
    }
}



