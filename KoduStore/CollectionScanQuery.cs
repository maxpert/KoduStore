using System;
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
            _lock.EnterReadLock();
            try
            {
                this.InternalScanFromStart(start, isPrimaryIdLookup, documents, docIds);
            }
            finally 
            {
                
                _lock.ExitReadLock();
            }
            
            // If it's already a primary Id lookup or no items found
            if (isPrimaryIdLookup || docIds.Count < 0)
            {
                return documents;
            }

            return this.GetMultiple(docIds);
        }

        private void InternalScanFromStart(object start, bool isPrimaryIdLookup, List<T> documents, SortedSet<Slice> docIds)
        {
            Slice startSlice = _documentFieldConverter.GetSliceFromMemberInfo(
                                        _memberInfo, 
                                        start, 
                                        lookupIndex: !isPrimaryIdLookup);
            Slice prefixSlice = _documentFieldConverter.GetSliceFromMemberInfo(
                                        _memberInfo, 
                                        null,
                                        lookupIndex: !isPrimaryIdLookup);
            byte[] prefixBytes = prefixSlice.ToByteArray();


            using (var snapshot = _db.GetSnapshot())
            using (var iterator = _db.NewIterator(new ReadOptions {FillCache = true, Snapshot = snapshot}))
            {
                iterator.Seek(startSlice);
                // Try seeking cursor to given start
                // If start == null then prefixSlice == startSlice
                // in that case we should use _direction to determine our start
                if (start == null ||
                    !iterator.Valid() ||
                    !iterator.Key().ToByteArray().HasPrefix(prefixBytes))
                {
                    this.SeekIteratorToBoundary(iterator, prefixSlice);
                }

                while (iterator.Valid())
                {
                    var k = iterator.Key();
                    var v = iterator.Value();

                    if (!k.ToByteArray().HasPrefix(prefixBytes))
                    {
                        break;
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

                    // Filled limit
                    // Or no seeking requested
                    if (collectedCount >= _limit || !this.MoveNextDirection(iterator))
                    {
                        break;
                    }
                }
            }
        }

        private void SeekIteratorToBoundary(Iterator iterator, Slice prefixSlice)
        {
            // Seek iterator to valid position based upon direction
            iterator.Seek(_direction > 0 ? _documentFieldConverter.GetKeyPrefixSlice() : prefixSlice);

            // Move one back in case of reverse since we see seeked +1 to the end
            if (iterator.Valid() && _direction > 0)
            {
                iterator.Prev();
            }
        }

        private bool MoveNextDirection(Iterator itr)
        {
            if (_direction > 0)
            {
                itr.Prev();
            }
            else if (_direction < 0)
            {
                itr.Next();
            }

            return _direction != 0;
        }
    }
}