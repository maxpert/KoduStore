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
                        if (serializer != null)
                        {
                            prefix = serializer.Serialize(idxInfo.Name);
                        }
                    }
                }
                else
                {
                    var keyTuple = _idMembers.FirstOrDefault(t => Equals(t.Item1, memberInfo));
                    serializer = keyTuple?.Item2.Serializer;
                    if (serializer != null)
                    {
                        prefix = serializer.Serialize(ClassNameHash);
                    }
                }
                
                writer.Write(lookupIndex ? IndexPrefix : IdPrefix);

                if (prefix != null)
                {
                    writer.Write(prefix);
                }

                if (value != null)
                {
                    var serializedBytes = serializer.Serialize(value);
                    this.VerifySerializedBytes(serializedBytes);
                    writer.Write(serializedBytes);
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
                    this.VerifySerializedBytes(serializedBytes);
                    writer.Write(serializedBytes);
                }

                writer.Flush();
                return Slice.FromByteArray(ms.ToArray());
            }
        }

        public IList<Slice> GetIndexKeySlices(T obj)
        {
            var keyTuple = this.GetKeySlice(obj);

            return _indexInfo.GetIndexMembersList()
                             .Select(indexMember => this.GetIndexKeySlice(obj, indexMember.Item1, indexMember.Item2, keyTuple))
                             .ToList();
        }

        private Slice GetIndexKeySlice(T obj, MemberInfo member, SecondaryIndexAttribute indexAttr, Slice primaryId)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                var serializer = indexAttr.Serializer;
                var serializedBytes = serializer.Serialize(this.GetValue(obj, member));
                this.VerifySerializedBytes(serializedBytes);
                writer.Write(IndexPrefix);
                writer.Write(ByteUtils.StringToBytes(indexAttr.Name));
                writer.Write(serializedBytes);
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

        private void VerifySerializedBytes(byte[] bytes)
        {
            if (bytes == null)
            {
                throw new InvalidDataException("Property serializer does not support specified value type");
            }
        }
    }
}
