using LevelDBWinRT;
using System.Collections.Generic;

namespace KoduStore
{
    class SliceComparer : IComparer<Slice>
    {
        public int Compare(Slice x, Slice y)
        {
            return ByteUtils.ByteArrayCompare(x.ToByteArray(deepCopy: false), y.ToByteArray(deepCopy: false));
        }
    }
}
