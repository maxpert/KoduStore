using LevelDBWinRT;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Windows.Storage;

namespace KoduStore
{
    public class Collection<K> where K : class
    {
        public enum ScanDirection
        {
            Forward = -1,
            Backward = 1,
        };

        private readonly string _path;

        private readonly ReaderWriterLockSlim _lock;

        private readonly IObjectSerializer<K> _serializer;

        private readonly DocumentConverter<K> _docConverter;
        
        private DB _db;

        public bool IsOpen => _db != null;

        public Collection(string dbPath) : this(dbPath, new BsonObjectSerializer<K>())
        {
        }

        public Collection(string dbPath, IObjectSerializer<K> serializer)
        {
            _serializer = serializer;
            _path = dbPath;
            _lock = new ReaderWriterLockSlim();
            _docConverter = new DocumentConverter<K>();
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

        public bool Put(K obj, bool flush = false)
        {
            return this.Put(new K[] { obj }, flush);
        }

        public bool Put(IEnumerable<K> objs, bool flush = false)
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

        public bool Delete(K item, bool flush = false)
        {
            return this.Delete(new K[] { item }, flush);
        }
        
        public bool Delete(IEnumerable<K> items, bool flush = false)
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

        public K Find<V>(Expression<Func<K, V>> fieldExpression, V value)
        {
            var foundList = this.FindRange<V>(fieldExpression, value, value);
            if (foundList.Count == 1)
            {
                return foundList[0];
            }

            return null;
        }

        public IList<K> FindMany<V>(Expression<Func<K, V>> fieldExpression, params V[] keys)
        {
            return this.FindMany(fieldExpression, keys);
        }

        public IList<K> FindMany<V>(Expression<Func<K, V>> fieldExpression, IEnumerable<V> keys)
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
        
        public IList<K> FindRange<V>(Expression<Func<K, V>> fieldExpression, V start, V end)
        {
            var documents = new List<K>();
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

        public IList<K> FindFrom<V>(Expression<Func<K, V>> fieldExpression, V start, ScanDirection direction = ScanDirection.Forward, int limit = int.MaxValue)
        {
            return this.InternalFindFrom(fieldExpression, start, direction, limit);
        }

        public IList<K> FindFrom<V>(Expression<Func<K, V>> fieldExpression, ScanDirection direction = ScanDirection.Forward, int limit = int.MaxValue)
        {
            return this.InternalFindFrom(fieldExpression, null, direction, limit);
        }

        private IList<K> InternalFindFrom<V>(Expression<Func<K, V>> fieldExpression, object start, ScanDirection direction = ScanDirection.Forward, int limit = int.MaxValue)
        {
            var documents = new List<K>();
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

        private IList<K> GetMultiple(IEnumerable<Slice> documentIds)
        {
            List<K> documents = new List<K>();
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

        private MemberInfo GetMemberInfoFromExpression<V>(Expression<Func<K, V>> fieldExpression)
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

        private void PutInBatch(WriteBatch batch, K obj)
        {
            var keySlice = _docConverter.GetKeySlice(obj);
            batch.Put(keySlice, Slice.FromByteArray(_serializer.Serialize(obj)));
            foreach (var i in _docConverter.GetIndexKeySlices(obj))
            {
                batch.Put(i, keySlice);
            }
        }

        private void DeletePreviousIndexesInBatch(WriteBatch batch, IEnumerable<K> objs)
        {
            IEnumerable<Slice> previousIds = objs.Select(o => _docConverter.GetKeySlice(o));
            IList<K> previousObjects = this.GetMultiple(previousIds);
            foreach (var i in previousObjects)
            {
                if (i != null)
                {
                    this.DeleteIndexesInBatch(batch, i);
                }
            }
        }

        private void DeleteIndexesInBatch(WriteBatch batch, K obj)
        {
            foreach (var i in _docConverter.GetIndexKeySlices(obj))
            {
                batch.Delete(i);
            }
        }
    }
}
