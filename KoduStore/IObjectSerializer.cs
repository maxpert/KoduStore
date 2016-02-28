using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    public interface IObjectSerializer<K> where K : class
    {
        byte[] Serialize(K doc);

        K Deserialize(byte[] bytes);
    }
}
