using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using LevelDBWinRT;

namespace KoduStore
{
    public sealed class CollectionScanQuery<T, V> : BaseCollectionQuery<T, V> where T : class
    {
        private int _limit;

        private int _direction;

        internal CollectionScanQuery(
            DB database, 
            ReaderWriterLockSlim readerLock, 
            MemberInfo memberInfo, 
            IObjectSerializer<T> objectSerializer, 
            DocumentConverter<T> documentConverter) : 
            base(database, readerLock, memberInfo, objectSerializer, documentConverter)
        {
            _limit = int.MaxValue;
            _direction = -1;
        }

        public CollectionScanQuery<T, V> Backward()
        {
            _direction = 1;
            return this;
        }

        public CollectionScanQuery<T, V> Forward()
        {
            _direction = -1;
            return this;
        }

        public CollectionScanQuery<T, V> Limit(int limit)
        {
            _limit = limit;
            return this;
        }

        public IList<T> GetAll(V start)
        {
            return this.InternalFindFrom(start);
        }

        public IList<T> GetAll()
        {
            return this.InternalFindFrom(null);
        } 

        private IList<T> InternalFindFrom(object start)
        {
            var documents = new List<T>();
            var comparer = new SliceComparer();
            var docIds = new SortedSet<Slice>(comparer);
            
            bool isPrimaryIdLookup = _documentFieldConverter.IsPrimaryIdField(_memberInfo);
            Slice startSlice = _documentFieldConverter.GetSliceFromMemberInfo(_memberInfo, start, lookupIndex: !isPrimaryIdLookup);
            byte[] prefixBytes = _documentFieldConverter.GetSliceFromMemberInfo(_memberInfo, null, lookupIndex: !isPrimaryIdLookup)
                                                        .ToByteArray();

            // Collect doc ids or documents (depending upon lookup type)
            this.OnEach(startSlice, (k, v) =>
            {
                // If prefix changed we crossed over (scanning should stop)
                if (!k.ToByteArray().HasPrefix(prefixBytes))
                {
                    return 0;
                }

                int collectedCount;
                if (isPrimaryIdLookup)
                {
                    documents.Add(_objectSerializer.Deserialize(v.ToByteArray()));
                    collectedCount = documents.Count;
                }
                else
                {
                    docIds.Add(v);
                    collectedCount = docIds.Count;
                }

                // Filled limit, should exit
                if (collectedCount >= _limit)
                {
                    return 0;
                }

                return _direction;
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