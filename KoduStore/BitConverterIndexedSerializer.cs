using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    internal class BitConverterIndexedSerializer : IPropertyValueSerializer
    {
        public static readonly BitConverterIndexedSerializer Singleton = new BitConverterIndexedSerializer();

        private static Dictionary<Type, Func<object, byte[]>> TypeSerializeMap =
            new Dictionary<Type, Func<object, byte[]>>()
            {
                [typeof(bool)] = p => BitConverter.GetBytes((bool)p),
                [typeof(char)] = p => BitConverter.GetBytes((char)p),
                [typeof(double)] = p => BitConverter.GetBytes((double)p),
                [typeof(short)] = p => BitConverter.GetBytes((short)p),
                [typeof(int)] = p => BitConverter.GetBytes((int)p),
                [typeof(long)] = p => BitConverter.GetBytes((long)p),
                [typeof(float)] = p => BitConverter.GetBytes((float)p),
                [typeof(ushort)] = p => BitConverter.GetBytes((ushort)p),
                [typeof(uint)] = p => BitConverter.GetBytes((uint)p),
                [typeof(ulong)] = p => BitConverter.GetBytes((ulong)p),
                [typeof(string)] = p =>
                {
                    var str = (string)p;
                    byte[] bytes = new byte[str.Length * sizeof(char)];
                    Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
                    return bytes;
                },
                [typeof(DateTime)] = p => BitConverter.GetBytes(((DateTime)p).ToBinary()),
                [typeof(DateTimeOffset)] = p => BitConverter.GetBytes(((DateTimeOffset)p).ToUnixTimeSeconds()),
                [typeof(byte[])] = p => (byte[])p,
                [typeof(byte)] = p => new byte[] { (byte)p },
                [typeof(Guid)] = p => ((Guid)p).ToByteArray(),
            };

        public byte[] Serialize(object field)
        {
            if (field == null)
            {
                throw new ArgumentNullException("field can not be null");
            }

            if (!TypeSerializeMap.ContainsKey(field.GetType()))
            {
                throw new InvalidCastException("Unable to serialize index field unknown type "+field.GetType().FullName);
            }

            return TypeSerializeMap[field.GetType()](field);
        }
    }
}
