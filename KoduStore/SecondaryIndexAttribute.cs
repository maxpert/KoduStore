using System;

namespace KoduStore
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class SecondaryIndexAttribute : Attribute
    {
        private Type _serializerType;

        internal IPropertyValueSerializer Serializer { get; private set; }

        public Type SerializerType
        {
            get { return _serializerType; }
            set
            {
                _serializerType = value;
                if (_serializerType != null)
                {
                    this.Serializer = (IPropertyValueSerializer)Activator.CreateInstance(_serializerType);
                }
            }
        }

        public string Name { get; set; } = string.Empty;

        public SecondaryIndexAttribute()
        {
            this.Serializer = BitConverterIndexedSerializer.Singleton;
        }
    }
}
