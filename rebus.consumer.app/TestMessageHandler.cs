using rebus.messages;
using Rebus.Handlers;

namespace rebus.consumer.app
{
    public class TestMessageHandler : IHandleMessages<TestMessage>
    {
        public TestMessageHandler()
        {
        }

        public async Task Handle(TestMessage message)
        {
            //throw new NotImplementedException();
            Console.WriteLine("TestMessage Received");
        }
    }
}
