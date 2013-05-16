using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusSimplifier.DemoClient
{
	class Program
	{
		static void Main(string[] args)
		{
			var serviceBus =
				ServiceBus.Setup(
					new InitializationRequest
					{
						Namespace = "YOUR NAMESPACE",
						Issuer = "YOR ISSUER",
						IssuerKey = "YOUR KEY"
					})
				.ClearTopics()
				.Subscribe<SimpleMessage>(HandleSimpleMessage, ReceiveMode.ReceiveAndDelete);

			Console.Write("Message: ");
			var message = Console.ReadLine();

			while (!string.IsNullOrEmpty(message))
			{
				serviceBus.Publish<SimpleMessage>(new SimpleMessage
				{
					Title = message,
					Id = Guid.NewGuid()
				});

				Console.Write("Message:");
				message = Console.ReadLine();
			}
		}

		static void HandleSimpleMessage(SimpleMessage msg)
		{
			Console.WriteLine(
				string.Format("Received '{0}' with message id of {1}", msg.Title, msg.Id)
				);
		}
	}

	public class SimpleMessage
	{
		public string Title { get; set; }
		public Guid Id { get; set; }
	}
}
