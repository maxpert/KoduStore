namespace KoduStore
{
    public interface IPropertyValueSerializer
    {
        byte[] Serialize(object field);
    }
}
