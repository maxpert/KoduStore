using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    internal static class ByteUtils
    {
        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memcmp(byte[] b1, byte[] b2, long count);

        public static int ByteArrayCompare(byte[] b1, byte[] b2)
        {
            // Validate buffers are the same length.
            // This also ensures that the count does not exceed the length of either buffer.  
            int cmp = memcmp(b1, b2, b1.Length < b2.Length ? b1.Length : b2.Length);
            if (cmp < 0)
            {
                cmp = -1;
            }
            else if (cmp > 0)
            {
                cmp = 1;
            }

            return cmp;
        }
    }
}
