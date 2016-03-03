using System;
using System.Collections.Generic;

namespace KoduStore
{
    internal class BitConverterIndexedSerializer : IPropertyValueSerializer
    {
        public static readonly BitConverterIndexedSerializer Singleton = new BitConverterIndexedSerializer();

        private static Dictionary<Type, Func<object, byte[]>> TYPE_SERIALIZE_MAP =
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
                [typeof(string)] = p => ByteUtils.StringToBytes((string)p),
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
                throw new ArgumentNullException("field");
            }

            if (!TYPE_SERIALIZE_MAP.ContainsKey(field.GetType()))
            {
                throw new InvalidCastException("Unable to serialize index field unknown type "+field.GetType().FullName);
            }

            return TYPE_SERIALIZE_MAP[field.GetType()](field);
        }
    }
}
