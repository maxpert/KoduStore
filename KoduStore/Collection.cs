using LevelDBWinRT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace KoduStore
{
    public class Collection<T> where T : class
    {
        public enum ScanDirection
        {
            Forward = -1,
            Backward = 1,
        };

        private readonly string _path;

        private readonly ReaderWriterLockSlim _lock;

        private readonly IObjectSerializer<T> _serializer;

        private readonly DocumentConverter<T> _docConverter;
        
        private DB _db;

        public bool IsOpen => _db != null;

        public Collection(string dbPath) : this(dbPath, new BsonObjectSerializer<T>())
        {
        }

        public Collection(string dbPath, IObjectSerializer<T> serializer)
        {
            _serializer = serializer;
            _path = dbPath;
            _lock = new ReaderWriterLockSlim();
            _docConverter = new DocumentConverter<T>();
        }

        public void Open(bool createIfMissing = true)
        {
            _db = new DB(new Options
            {
                CreateIfMissing = true,
                Compressor = CompressorType.Snappy,
            },
            _path);
        }

        public void Close()
        {
            _lock.Dispose();
            _db.Dispose();
            _db = null;
        }

        public bool Put(T obj, bool flush = false)
        {
            return this.Put(new[] { obj }, flush);
        }

        public bool Put(IEnumerable<T> objs, bool flush = false)
        {
            try
            {
                using (var batch = new WriteBatch())
                {
                    this.DeletePreviousIndexesInBatch(batch, objs);
                    foreach(var obj in objs)
                    {
                        this.PutInBatch(batch, obj);
                    }
                    
                    _lock.EnterWriteLock();
                    return _db.Write(new WriteOptions { Sync = flush }, batch);
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool Delete(T item, bool flush = false)
        {
            return this.Delete(new T[] { item }, flush);
        }
        
        public bool Delete(IEnumerable<T> items, bool flush = false)
        {
            try
            {
                _lock.EnterWriteLock();

                using (var batch = new WriteBatch())
                {
                    foreach (var item in items)
                    {
                        var keySlice = _docConverter.GetKeySlice(item);
                        batch.Delete(keySlice);
                        this.DeleteIndexesInBatch(batch, item);
                    }

                    return _db.Write(new WriteOptions { Sync = flush }, batch);
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public T Find<V>(Expression<Func<T, V>> fieldExpression, V value)
        {
            var foundList = this.FindRange(fieldExpression, value, value);
            if (foundList.Count == 1)
            {
                return foundList[0];
            }

            return null;
        }

        public IList<T> FindMany<V>(Expression<Func<T, V>> fieldExpression, params V[] keys)
        {
            return this.FindMany(fieldExpression, new List<V>(keys));
        }

        public IList<T> FindMany<V>(Expression<Func<T, V>> fieldExpression, IEnumerable<V> keys)
        {
            var comparer = new SliceComparer();
            var docIds = new SortedSet<Slice>(comparer);

            MemberInfo memberInfo = this.GetMemberInfoFromExpression(fieldExpression);
            bool isPrimaryIdLookup = _docConverter.IsPrimaryIdField(memberInfo);
            foreach (var key in keys)
            {
                docIds.Add(_docConverter.GetSliceFromMemberInfo(memberInfo, key, lookupIndex: !isPrimaryIdLookup));
            }

            return this.GetMultiple(docIds);
        }
        
        public IList<T> FindRange<V>(Expression<Func<T, V>> fieldExpression, V start, V end)
        {
            var documents = new List<T>();
            var comparer = new SliceComparer();
            var docIds = new SortedSet<Slice>(comparer);

            MemberInfo memberInfo = this.GetMemberInfoFromExpression(fieldExpression);
            bool isPrimaryIdLookup = _docConverter.IsPrimaryIdField(memberInfo);
            Slice startSlice = _docConverter.GetSliceFromMemberInfo(memberInfo, start, lookupIndex: !isPrimaryIdLookup);
            Slice endSlice = _docConverter.GetSliceFromMemberInfo(memberInfo, end, lookupIndex: !isPrimaryIdLookup);
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
                    documents.Add(_serializer.Deserialize(v.ToByteArray()));
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

        public IList<T> FindFrom<V>(Expression<Func<T, V>> fieldExpression, V start, ScanDirection direction = ScanDirection.Forward, int limit = int.MaxValue)
        {
            return this.InternalFindFrom(fieldExpression, start, direction, limit);
        }

        public IList<T> FindFrom<V>(Expression<Func<T, V>> fieldExpression, ScanDirection direction = ScanDirection.Forward, int limit = int.MaxValue)
        {
            return this.InternalFindFrom(fieldExpression, null, direction, limit);
        }

        private IList<T> InternalFindFrom<V>(Expression<Func<T, V>> fieldExpression, object start, ScanDirection direction = ScanDirection.Forward, int limit = int.MaxValue)
        {
            var documents = new List<T>();
            var comparer = new SliceComparer();
            var docIds = new SortedSet<Slice>(comparer);

            MemberInfo memberInfo = this.GetMemberInfoFromExpression(fieldExpression);
            bool isPrimaryIdLookup = _docConverter.IsPrimaryIdField(memberInfo);
            Slice startSlice = _docConverter.GetSliceFromMemberInfo(memberInfo, start, lookupIndex: !isPrimaryIdLookup);
            byte[] prefixBytes = _docConverter.GetSliceFromMemberInfo(memberInfo, null, lookupIndex: !isPrimaryIdLookup)
                                              .ToByteArray();

            // Collect doc ids or documents (depending upon lookup type)
            this.OnEach(startSlice, (k, v) =>
            {
                // If prefix changed we crossed over (scanning should stop)
                if (!k.ToByteArray().HasPrefix(prefixBytes))
                {
                    return 0;
                }

                int count = 0;
                if (isPrimaryIdLookup)
                {
                    documents.Add(_serializer.Deserialize(v.ToByteArray()));
                    count = documents.Count;
                }
                else
                {
                    docIds.Add(v);
                    count = docIds.Count;
                }

                // Filled limit, should exit
                if (count >= limit)
                {
                    return 0;
                }

                return (int)direction;
            });

            // If it's already a primary Id lookup or no items found
            if (isPrimaryIdLookup || docIds.Count < 0)
            {
                return documents;
            }

            return this.GetMultiple(docIds);
        }

        private IList<T> GetMultiple(IEnumerable<Slice> documentIds)
        {
            List<T> documents = new List<T>();
            SliceComparer comparer = new SliceComparer();
            this.WithIterator(iterator =>
            {
                foreach (var id in documentIds)
                {
                    iterator.Seek(id);
                    if (iterator.Valid() && comparer.Compare(id, iterator.Key()) == 0)
                    {
                        documents.Add(_serializer.Deserialize(iterator.Value().ToByteArray()));
                    }
                    else
                    {
                        documents.Add(null);
                    }
                }
            });

            return documents;
        }

        private void OnEach(Slice start, Func<Slice, Slice, int> callback)
        {
            this.WithIterator(iterator =>
            {
                iterator.Seek(start);
                while (iterator.Valid())
                {
                    var move = callback(iterator.Key(), iterator.Value());

                    if (move == 0)
                    {
                        return;
                    }

                    if (move < 0)
                    {
                        iterator.Next();
                    }
                    else
                    {
                        iterator.Prev();
                    }
                }
            });
        }

        private void WithIterator(Action<Iterator> callback)
        {
            try
            {
                _lock.EnterReadLock();
                using (var iterator = _db.NewIterator(new ReadOptions { Snapshot = _db.GetSnapshot() }))
                {
                    callback(iterator);
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        private MemberInfo GetMemberInfoFromExpression<V>(Expression<Func<T, V>> fieldExpression)
        {
            MemberInfo ret = null;
            if (fieldExpression.NodeType == ExpressionType.Lambda && fieldExpression.Body is MemberExpression)
            {
                var operand = fieldExpression.Body as MemberExpression;
                ret = operand?.Member;
            }

            if (ret == null)
            {
                throw new InvalidOperationException("Invalid expression " + fieldExpression.ToString());
            }

            return ret;
        }

        private void PutInBatch(WriteBatch batch, T obj)
        {
            var keySlice = _docConverter.GetKeySlice(obj);
            batch.Put(keySlice, Slice.FromByteArray(_serializer.Serialize(obj)));
            foreach (var i in _docConverter.GetIndexKeySlices(obj))
            {
                batch.Put(i, keySlice);
            }
        }

        private void DeletePreviousIndexesInBatch(WriteBatch batch, IEnumerable<T> objs)
        {
            IEnumerable<Slice> previousIds = objs.Select(o => _docConverter.GetKeySlice(o));
            IList<T> previousObjects = this.GetMultiple(previousIds);
            foreach (var i in previousObjects)
            {
                if (i != null)
                {
                    this.DeleteIndexesInBatch(batch, i);
                }
            }
        }

        private void DeleteIndexesInBatch(WriteBatch batch, T obj)
        {
            foreach (var i in _docConverter.GetIndexKeySlices(obj))
            {
                batch.Delete(i);
            }
        }
    }
}
