using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using LevelDBWinRT;

namespace KoduStore
{
    public sealed class CollectionKeysQuery<T, V> : BaseCollectionQuery<T, V> where T : class
    {
        internal CollectionKeysQuery(
            DB database, 
            ReaderWriterLockSlim readerLock, 
            MemberInfo memberInfo, 
            IObjectSerializer<T> objectSerializer, 
            DocumentConverter<T> documentConverter) : 
            base(database, readerLock, memberInfo, objectSerializer, documentConverter)
        {
        }

        public IList<T> Get(params V[] keys)
        {
            return this.GetAll(new List<V>(keys));
        }

        public IList<T> GetAll(IEnumerable<V> keys)
        {
            var comparer = new SliceComparer();
            var docIds = new SortedSet<Slice>(comparer);

            bool isPrimaryIdLookup = _documentFieldConverter.IsPrimaryIdField(_memberInfo);
            foreach (var key in keys)
            {
                docIds.Add(
                    _documentFieldConverter.GetSliceFromMemberInfo(
                        _memberInfo, 
                        key, 
                        lookupIndex: !isPrimaryIdLookup
                    )
                );
            }

            return this.GetDocumentsWithLock(docIds);
        }

        private IList<T> GetDocumentsWithLock(IEnumerable<Slice> documentIds)
        {
            List<T> documents = new List<T>();
            SliceComparer comparer = new SliceComparer();

            foreach (var id in documentIds)
            {
                this.WithIterator(iterator =>
                {
                    iterator.Seek(id);
                    if (iterator.Valid() && comparer.Compare(id, iterator.Key()) == 0)
                    {
                        documents.Add(_objectSerializer.Deserialize(iterator.Value().ToByteArray(deepCopy: false)));
                    }
                    else
                    {
                        documents.Add(null);
                    }

                });
            }

            return documents;
        }
    }
}