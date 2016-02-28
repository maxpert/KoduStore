using LevelDBWinRT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    class SliceComparer : IComparer<Slice>
    {
        public int Compare(Slice x, Slice y)
        {
            return ByteUtils.ByteArrayCompare(x.ToByteArray(), y.ToByteArray());
        }
    }
}
