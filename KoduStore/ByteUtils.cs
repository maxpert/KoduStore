using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading.Tasks;
using Windows.Security.Cryptography;
using Windows.Security.Cryptography.Core;
using Windows.Storage.Streams;

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

        public static bool HasPrefix(this byte[] a, byte[] b)
        {
            if (a.Length < b.Length)
            {
                return false;
            }

            return memcmp(a, b, b.Length) == 0;
        }

        public static byte[] Hash(byte[] bytes)
        {
            var input = CryptographicBuffer.CreateFromByteArray(bytes);
            var algorithm = HashAlgorithmProvider.OpenAlgorithm(HashAlgorithmNames.Md5);
            var hashed = algorithm.HashData(input);
            return hashed.ToArray();
        }


        public static byte[] StringToBytes(string str)
        {
            byte[] bytes = new byte[str.Length * sizeof(char)];
            System.Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return bytes;
        }
    }
}
