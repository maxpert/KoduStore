using LevelDBWinRT;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace KoduStore
{
    internal class DocumentConverter<T> where T : class
    {
        private const byte IdPrefix = 255;

        private const byte IndexPrefix = 254;

        private static readonly byte[] ClassNameHash = ByteUtils.Hash(ByteUtils.StringToBytes(typeof(T).FullName));

        private static readonly ConcurrentDictionary<string, byte[]> IndexNamesCache = new ConcurrentDictionary<string, byte[]>();

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
                IPropertyValueSerializer serializer = null;

                if (lookupIndex)
                {
                    if (_indexInfo.IndexAttributeMap.ContainsKey(memberInfo))
                    {
                        var idxInfo = _indexInfo.IndexAttributeMap[memberInfo][0];
                        serializer = idxInfo.Serializer;
                        if (serializer != null)
                        {
                            writer.Write(this.GetIndexKeyPrefixSlice(idxInfo).ToByteArray(deepCopy: false));
                        }
                    }
                }
                else
                {
                    var keyTuple = _idMembers.FirstOrDefault(t => Equals(t.Item1, memberInfo));
                    serializer = keyTuple?.Item2.Serializer;
                    if (serializer != null)
                    {
                        writer.Write(this.GetKeyPrefixSlice().ToByteArray(deepCopy: false));
                    }
                }

                if (serializer == null)
                {
                    throw new InvalidOperationException("Serializer for filed "+memberInfo.Name+ " is null");
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
                writer.Write(this.GetKeyPrefixSlice().ToByteArray(deepCopy: false));
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
        
        public Slice GetKeyPrefixSlice()
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                writer.Write(ClassNameHash);
                writer.Write(IdPrefix);
                writer.Flush();

                return Slice.FromByteArray(ms.ToArray());
            }
        }

        private Slice GetIndexKeyPrefixSlice(SecondaryIndexAttribute indexAttr)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                writer.Write(ClassNameHash);
                writer.Write(IndexPrefix);
                writer.Write(this.HashedIndexName(indexAttr.Name));
                writer.Flush();

                return Slice.FromByteArray(ms.ToArray());
            }
        }

        private Slice GetIndexKeySlice(T obj, MemberInfo member, SecondaryIndexAttribute indexAttr, Slice primaryId)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                var serializer = indexAttr.Serializer;
                var serializedBytes = serializer.Serialize(this.GetValue(obj, member));
                this.VerifySerializedBytes(serializedBytes);

                writer.Write(this.GetIndexKeyPrefixSlice(indexAttr).ToByteArray(deepCopy: false));
                writer.Write(serializedBytes);
                writer.Write(primaryId.ToByteArray(deepCopy: false));
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

        private byte[] HashedIndexName(string indexAttrName)
        {
            return DocumentConverter<T>.IndexNamesCache.GetOrAdd(
                indexAttrName,
                _ => ByteUtils.Hash(ByteUtils.StringToBytes(indexAttrName)));
        }
    }
}
