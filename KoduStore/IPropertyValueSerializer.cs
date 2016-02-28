using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    public interface IPropertyValueSerializer
    {
        byte[] Serialize(object field);
    }
}
