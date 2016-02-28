using LevelDBWinRT;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    internal class DocumentConverter<T> where T : class
    {
        private const byte IdPrefix = 255;

        private const byte IndexPrefix = 254;

        private static readonly byte[] ClassNameHash = ByteUtils.Hash(ByteUtils.StringToBytes(typeof(T).FullName));

        private readonly IndexInfo _indexInfo;

        private IList<Tuple<MemberInfo, PrimaryIndexAttribute>> _idMembers;

        public DocumentConverter()
        {
            _indexInfo = new IndexInfo(typeof(T));
            _idMembers = typeof(T).GetMembers(BindingFlags.Instance | BindingFlags.Public)
                                .Cast<MemberInfo>()
                                .Union(typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
                                .Where(m => m.GetCustomAttribute<PrimaryIndexAttribute>() != null)
                                .Select(m => new Tuple<MemberInfo, PrimaryIndexAttribute>(m, m.GetCustomAttribute<PrimaryIndexAttribute>()))
                                .ToList();
            
            this.EnsureDocumentKey();
        }

        public void EnsureDocumentKey()
        {
            if (_idMembers.Count != 1)
            {
                throw new InvalidOperationException("Document type " + typeof(T).FullName + " should have only one primary id");
            }
        }

        public bool IsPrimaryIdField(MemberInfo info)
        {
            return _idMembers.Any(p => p.Item1 == info);
        }
        
        public Slice GetSliceFromMemberInfo(MemberInfo memberInfo, object value, bool lookupIndex)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                byte[] prefix = null;
                IPropertyValueSerializer serializer = null;

                if (lookupIndex)
                {
                    if (_indexInfo.IndexAttributeMap.ContainsKey(memberInfo))
                    {
                        var idxInfo = _indexInfo.IndexAttributeMap[memberInfo][0];
                        serializer = idxInfo.Serializer;
                        prefix = serializer.Serialize(idxInfo.Name);
                    }
                }
                else
                {
                    var keyTuple = _idMembers.FirstOrDefault(t => t.Item1 == memberInfo);
                    serializer = keyTuple?.Item2.Serializer;
                    prefix = serializer.Serialize(ClassNameHash);
                }

                if (serializer == null)
                {
                    throw new InvalidOperationException(memberInfo.Name + " has no index and it's not a primary key to do a lookup");
                }

                writer.Write(lookupIndex ? IndexPrefix : IdPrefix);

                if (prefix != null)
                {
                    writer.Write(prefix);
                }

                if (value != null)
                {
                    writer.Write(serializer.Serialize(value));
                }
                
                writer.Flush();

                return Slice.FromByteArray(ms.ToArray());
            }
        }

        public Slice GetKeySlice(T obj)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                writer.Write(IdPrefix);
                writer.Write(ClassNameHash);
                foreach (var idMember in _idMembers)
                {
                    var serializer = idMember.Item2.Serializer;
                    var serializedBytes = serializer.Serialize(this.GetValue(obj, idMember.Item1));
                    writer.Write(serializedBytes);
                }

                writer.Flush();
                return Slice.FromByteArray(ms.ToArray());
            }
        }

        public IList<Slice> GetIndexKeySlices(T obj)
        {
            var tuples = new List<Slice>();
            var keyTuple = this.GetKeySlice(obj);
            foreach (var indexMember in _indexInfo.GetIndexMembersList())
            {
                tuples.Add(this.GetIndexKeySlice(obj, indexMember.Item1, indexMember.Item2, keyTuple));
            }

            return tuples;
        }

        private Slice GetIndexKeySlice(T obj, MemberInfo member, SecondaryIndexAttribute indexAttr, Slice primaryId)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                var serializer = indexAttr.Serializer;
                writer.Write(IndexPrefix);
                writer.Write(ByteUtils.StringToBytes(indexAttr.Name));
                writer.Write(serializer.Serialize(this.GetValue(obj, member)));
                writer.Write(primaryId.ToByteArray());
                writer.Flush();

                return Slice.FromByteArray(ms.ToArray());
            }
        }

        private object GetValue(T obj, MemberInfo info)
        {
            if (info is PropertyInfo)
            {
                return ((PropertyInfo)info).GetValue(obj);
            }

            return ((FieldInfo)info).GetValue(obj);
        }
    }
}
