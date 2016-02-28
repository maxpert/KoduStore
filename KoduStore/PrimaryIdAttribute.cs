using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public class PrimaryIdAttribute : Attribute
    {
        public IPropertyValueSerializer Serializer { get; set; } = BitConverterIndexedSerializer.Singleton;
    }
}
