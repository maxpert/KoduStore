namespace KoduStore
{
    public interface IObjectSerializer<K> where K : class
    {
        byte[] Serialize(K doc);

        K Deserialize(byte[] bytes);
    }
}
