using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using LevelDBWinRT;

namespace KoduStore
{
    public abstract class BaseCollectionQuery<T, V> : IDisposable, ICollectionQuery<T, V> where T : class
    {
        internal DB _db;

        internal ReaderWriterLockSlim _lock;

        internal IObjectSerializer<T> _objectSerializer;

        internal DocumentConverter<T> _documentFieldConverter;

        internal MemberInfo _memberInfo;

        internal Snapshot _snapshot;

        internal BaseCollectionQuery(
            DB database, 
            ReaderWriterLockSlim readerLock, 
            MemberInfo memberInfo,
            IObjectSerializer<T> objectSerializer,
            DocumentConverter<T> documentConverter)
        {
            _db = database;
            _lock = readerLock;
            _memberInfo = memberInfo;
            _objectSerializer = objectSerializer;
            _documentFieldConverter = documentConverter;
            _snapshot = _db.GetSnapshot();
        }

        public void Dispose()
        {
            _db.ReleaseSnapshot(_snapshot);
            _db = null;
            _lock = null;
            _memberInfo = null;
            _objectSerializer = null;
            _documentFieldConverter = null;
            _snapshot = null;

            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void OnEach(Slice start, Func<Slice, Slice, int> callback)
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

        protected virtual void Dispose(bool disposing)
        {
        }

        protected void WithIterator(Action<Iterator> callback)
        {
            try
            {
                _lock.EnterReadLock();
                using (var iterator = _db.NewIterator(new ReadOptions { Snapshot = _snapshot }))
                {
                    callback(iterator);
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
        
        protected IList<T> GetMultiple(IEnumerable<Slice> documentIds)
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
                        documents.Add(_objectSerializer.Deserialize(iterator.Value().ToByteArray()));
                    }
                    else
                    {
                        documents.Add(null);
                    }
                }
            });

            return documents;
        }
    }
}