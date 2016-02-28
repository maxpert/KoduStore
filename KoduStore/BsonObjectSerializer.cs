using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    internal class BsonObjectSerializer<K> : IObjectSerializer<K> where K : class
    {
        public K Deserialize(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            using (var reader = new BsonReader(ms))
            {
                var serializer = new JsonSerializer();
                return serializer.Deserialize<K>(reader);
            }
        }

        public byte[] Serialize(K doc)
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
