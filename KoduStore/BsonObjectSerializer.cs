using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using System;
using System.IO;

namespace KoduStore
{
    internal class BsonObjectSerializer<T> : IObjectSerializer<T> where T : class
    {
        public T Deserialize(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            using (var reader = new BsonReader(ms))
            {
                var serializer = new JsonSerializer();
                return serializer.Deserialize<T>(reader);
            }
        }

        public byte[] Serialize(T doc)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BsonWriter(ms))
            {
                var serializer = new JsonSerializer();
                serializer.Serialize(writer, doc);
                writer.Flush();
                return ms.ToArray();
            }
        }
    }
}
