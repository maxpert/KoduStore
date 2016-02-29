using System;

namespace KoduStore
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class PrimaryIndexAttribute : Attribute
    {
        private Type _serializerType;

        internal virtual IPropertyValueSerializer Serializer { get; private set; }

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

        public PrimaryIndexAttribute()
        {
            this.Serializer = BitConverterIndexedSerializer.Singleton;
        }
    }
}
