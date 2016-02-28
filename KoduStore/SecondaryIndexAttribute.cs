using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
    public class SecondaryIndexAttribute : Attribute
    {
        public Type FieldType { get; set; }

        public IPropertyValueSerializer Serializer { get; set; } = BitConverterIndexedSerializer.Singleton;

        public string Name { get; set; } = string.Empty;
    }
}
