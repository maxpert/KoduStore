using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using KoduStore;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace CRUDTests
{
    [TestClass]
    public class CustomSerializersTest
    {
        private static int ACCUMULATOR = 0;

        public class CustomPropertySerializer : IPropertyValueSerializer
        {
            public static readonly CustomPropertySerializer Instance = new CustomPropertySerializer();

            public byte[] Serialize(object field)
            {
                using (var ms = new MemoryStream())
                using (var writer = new BsonWriter(ms))
                {
                    var serializer = new JsonSerializer();
                    serializer.Serialize(writer, new[] { field });
                    writer.Flush();
                    return ms.ToArray();
                }
            }
        }

        [DataContract]
        private class NoIdObject
        {
            [DataMember]
            [SecondaryIndex(SerializerType = typeof(CustomPropertySerializer))]
            public string Name { get; set; }
        }

        [DataContract]
        private class TestInternalObject : NoIdObject
        {
            [DataMember]
            [PrimaryIndex(SerializerType = typeof(CustomPropertySerializer))]
            public int Id;
        }

        private Collection<TestInternalObject> _collection;

        [TestInitialize]
        public void OnInitialize()
        {
            ACCUMULATOR++;
            _collection = new Collection<TestInternalObject>($"custom_serializer_{ACCUMULATOR}");
            _collection.Open();
        }

        [TestCleanup]
        public void OnCleanup()
        {
            if (_collection.IsOpen)
            {
                _collection.Close();
                _collection = null;
            }
        }

        [TestMethod]
        public void TestCustomSerializer()
        {
            var t = new TestInternalObject() {Id = 0, Name = "Hello"};
            _collection.Put(t);

            var query = _collection.Query(o => o.Id);
            Assert.IsNotNull(query.Get(0).FirstOrDefault(), "Unable to find o");
            query.Dispose();
        }

        [TestMethod]
        public void TestNoIdThrowsException()
        {
            try
            {
                var t = new NoIdObject() { Name = "Hello" };
                var collection = new Collection<NoIdObject>("no_id");
                collection.Put(t);
            }
            catch (InvalidOperationException)
            {
                return;
            }
            
            Assert.Fail("Exception was not thrown when it was expected");
        }
    }
}