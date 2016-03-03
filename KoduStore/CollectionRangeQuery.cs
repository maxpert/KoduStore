using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using LevelDBWinRT;

namespace KoduStore
{
    public sealed class CollectionRangeQuery<T, V> : BaseCollectionQuery<T, V> where T : class
    {
        internal CollectionRangeQuery(
            DB database, 
            ReaderWriterLockSlim readerLock, 
            MemberInfo memberInfo, 
            IObjectSerializer<T> objectSerializer, 
            DocumentConverter<T> documentConverter) : 
            base(database, readerLock, memberInfo, objectSerializer, documentConverter)
        {
        }

        public IList<T> Get(V start, V end)
        {
            var documents = new List<T>();
            var comparer = new SliceComparer();
            var docIds = new SortedSet<Slice>(comparer);
            
            bool isPrimaryIdLookup = _documentFieldConverter.IsPrimaryIdField(_memberInfo);
            Slice startSlice = _documentFieldConverter.GetSliceFromMemberInfo(_memberInfo, start, lookupIndex: !isPrimaryIdLookup);
            Slice endSlice = _documentFieldConverter.GetSliceFromMemberInfo(_memberInfo, end, lookupIndex: !isPrimaryIdLookup);
            int direction = comparer.Compare(startSlice, endSlice);

            // Collect doc ids or documents (depending upon lookup type)
            this.OnEach(startSlice, (k, v) =>
            {
                int cmp = comparer.Compare(k, endSlice);
                if (cmp != 0 && cmp != direction)
                {
                    return 0;
                }

                if (isPrimaryIdLookup)
                {
                    documents.Add(_objectSerializer.Deserialize(v.ToByteArray()));
                }
                else
                {
                    docIds.Add(v);
                }

                // Keep moving forward for start <= end
                // Move backwards start > end
                return direction > 0 ? 1 : -1;
            });

            // If it's already a primary Id lookup or no items found
            if (isPrimaryIdLookup || docIds.Count < 0)
            {
                return documents;
            }

            return this.GetMultiple(docIds);
        }
    }
}
