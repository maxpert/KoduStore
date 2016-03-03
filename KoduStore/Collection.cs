using LevelDBWinRT;
using System;
using System.Collections.Generic;
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
        
        public CollectionRangeQuery<T, V> QueryRange<V>(Expression<Func<T, V>> fieldExpression)
        {
            return new CollectionRangeQuery<T, V>(
                _db, 
                _lock, 
                this.GetMemberInfoFromExpression(fieldExpression), 
                _serializer, 
                _docConverter
            );
        }

        public CollectionKeysQuery<T, V> Query<V>(Expression<Func<T, V>> fieldExpression)
        {
            return new CollectionKeysQuery<T, V>(
                _db,
                _lock,
                this.GetMemberInfoFromExpression(fieldExpression),
                _serializer,
                _docConverter);
        }

        public CollectionScanQuery<T, V> QueryScan<V>(Expression<Func<T, V>> fieldExpression)
        {
            return new CollectionScanQuery<T, V>(
                _db,
                _lock,
                this.GetMemberInfoFromExpression(fieldExpression),
                _serializer,
                _docConverter);
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
            var snapshot = _db.GetSnapshot();
            try
            {
                var readOption = new ReadOptions { Snapshot = snapshot, FillCache = true };
                
                foreach (var obj in objs)
                {
                    var keySlice = _docConverter.GetKeySlice(obj);
                    var prevObjSlice = _db.Get(readOption, keySlice);
                    if (prevObjSlice == null)
                    {
                        continue;
                    }
                    
                    var prevObj = _serializer.Deserialize(prevObjSlice.ToByteArray());
                    this.DeleteIndexesInBatch(batch, prevObj);
                }
            }
            finally
            {
                _db.ReleaseSnapshot(snapshot);
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
