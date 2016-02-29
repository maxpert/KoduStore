using System;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using KoduStore;
using System.Runtime.Serialization;
using System.Collections.Generic;
using System.Linq;

namespace CRUDTests
{
    [TestClass]
    public class BasicCRUDTest
    {
        private static int ACCUMULATOR = 0;

        [DataContract]
        private class BasicObject
        {
            [DataMember]
            [PrimaryIndex]
            public int Id;

            [DataMember]
            [SecondaryIndex]
            public int SecondaryIndex { get; set; } = new Random().Next(100000);

            [DataMember]
            public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
        }

        private Collection<BasicObject> _collection;

        [TestInitialize]
        public void OnInitialize()
        {
            ACCUMULATOR++;
            _collection = new Collection<BasicObject>($"basic_crud_{ACCUMULATOR}");
            _collection.Open();
        }

        [TestCleanup]
        public void OnCleanup()
        {
            if (_collection.IsOpen)
            {
                _collection.Close();
                _collection = null;
            }
        }

        [TestMethod]
        public void TestBasicPut()
        {
            var success = _collection.Put(new BasicObject { Id = new Random().Next(10, 1000000) });
            Assert.IsTrue(success, "Unable to save entry");
        }

        [TestMethod]
        public void TestBatchPut()
        {
            var success = _collection.Put(new BasicObject[]
            {
                new BasicObject { Id = new Random().Next(10, 1000000) },
                new BasicObject { Id = new Random().Next(10, 1000000) },
            });

            Assert.IsTrue(success, "Unable to save entries");
        }

        [TestMethod]
        public void TestBatchPut100RandomEntries()
        {
            Assert.IsTrue(_collection.Put(this.CreateRandomBasicObjects(100)), "Unable to add batch entries");
        }

        [TestMethod]
        public void TestPutClearsIndexes()
        {
            var entry = new BasicObject { Id = 10, SecondaryIndex = 30 };
            _collection.Put(entry);

            entry.SecondaryIndex = 40;
            entry.Created = DateTimeOffset.Now;
            Assert.IsTrue(_collection.Put(entry));
            Assert.IsNull(_collection.Find(p => p.SecondaryIndex, 30), "Index was not cleared for updated object");
        }

        [TestMethod]
        public void TestBasicFind()
        {
            var insertedObj = new BasicObject
            {
                Id = 0,
            };

            var inserted = _collection.Put(insertedObj);
            var item = _collection.Find(p => p.Id, 0);

            Assert.IsTrue(inserted, "Unable to add entry");
            Assert.IsNotNull(item, "Unable to find entry");
            Assert.AreEqual(insertedObj.Created.ToString(), item.Created.ToString(), "Correct objects were not serialized");
        }
        
        [TestMethod]
        public void TestBasicFindMissing()
        {
            var item = _collection.Find(p => p.Id, -1000);
            Assert.IsNull(item, "Unable to find entry");
        }

        [TestMethod]
        public void TestFindIdRange()
        {
            var items = this.CreateRandomBasicObjects(30);
            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindRange(p => p.Id, 0, items.Count - 1);
            SortedSet<int> foundItemIds = new SortedSet<int>(foundItems.Select(i => i.Id));
            SortedSet<int> itemIds = new SortedSet<int>(items.Select(i => i.Id));

            Assert.IsTrue(foundItemIds.SetEquals(itemIds), "Unable to find all items");
        }
        
        [TestMethod]
        public void TestFindIdRangeBackwards()
        {
            var items = this.CreateRandomBasicObjects(30);
            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindRange(p => p.Id, items.Count - 1, 0);
            SortedSet<int> foundItemIds = new SortedSet<int>(foundItems.Select(i => i.Id));
            SortedSet<int> itemIds = new SortedSet<int>(items.Select(i => i.Id));

            Assert.IsTrue(foundItemIds.SetEquals(itemIds), "Unable to find all items");
        }

        [TestMethod]
        public void TestFindFromForwards()
        {
            var items = this.CreateRandomBasicObjects(30);
            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindFrom(p => p.Id);
            SortedSet<int> foundItemIds = new SortedSet<int>(foundItems.Select(i => i.Id));
            SortedSet<int> itemIds = new SortedSet<int>(items.Select(i => i.Id));

            Assert.IsTrue(foundItemIds.SetEquals(itemIds), "Unable to find all items");
        }
        
        [TestMethod]
        public void TestFindFromBackwards()
        {
            var items = this.CreateRandomBasicObjects(30);
            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindFrom(p => p.Id, 29, Collection<BasicObject>.ScanDirection.Backward);
            SortedSet<int> foundItemIds = new SortedSet<int>(foundItems.Select(i => i.Id));
            SortedSet<int> itemIds = new SortedSet<int>(items.Select(i => i.Id));

            Assert.IsTrue(foundItemIds.SetEquals(itemIds), "Unable to find all items");
        }

        [TestMethod]
        public void TestFindFromLimitTest()
        {
            var items = this.CreateRandomBasicObjects(30);
            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindFrom(p => p.Id, 0, limit: 10);

            Assert.IsTrue(foundItems.Count == 10, "Unable to find all items");
        }

        [TestMethod]
        public void TestFindFromLimitBackwardsTest()
        {
            var items = this.CreateRandomBasicObjects(30);
            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindFrom(p => p.Id, 10, Collection<BasicObject>.ScanDirection.Backward, limit: 10);

            Assert.IsTrue(foundItems.Count == 10, "Unable to find all items");
        }
        
        [TestMethod]
        public void TestFindFromLessThanLimitTest()
        {
            var items = this.CreateRandomBasicObjects(30);
            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindFrom(p => p.Id, 25, limit: 10);

            Assert.IsTrue(foundItems.Count < 10, "Unable to find all items");
        }

        [TestMethod]
        public void TestFindFromSecondaryIndexDupTest()
        {
            var items = this.CreateRandomBasicObjects(30);
            foreach (var item in items)
            {
                item.SecondaryIndex = item.Id / 4;
            }

            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindFrom(p => p.SecondaryIndex);
            Assert.IsTrue(foundItems.Count == 30, "Unable to find all items");
        }
        
        [TestMethod]
        public void TestFindFromSecondaryIndexLimitTest()
        {
            var items = this.CreateRandomBasicObjects(30);
            foreach (var item in items)
            {
                item.SecondaryIndex = item.Id / 4;
            }

            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindFrom(p => p.SecondaryIndex, limit: 5);
            Assert.IsTrue(foundItems.Count == 5, "Unable to find all items");
        }

        [TestMethod]
        public void TestFindFromSecondaryIndexTest()
        {
            var items = this.CreateRandomBasicObjects(30);
            foreach (var item in items)
            {
                item.SecondaryIndex = item.Id;
            }

            _collection.Put(items);

            IList<BasicObject> foundItems = _collection.FindFrom(p => p.SecondaryIndex);
            Assert.IsTrue(foundItems.Count == 30, "Unable to find all items");
        }

        [TestMethod]
        public void TestFindSecondaryIndex()
        {
            var insertedObj = new BasicObject
            {
                Id = 1860,
                SecondaryIndex = 200,
            };

            var inserted = _collection.Put(insertedObj);
            var items = _collection.FindRange(p => p.SecondaryIndex, insertedObj.SecondaryIndex, insertedObj.SecondaryIndex);

            Assert.IsTrue(items.Count == 1, "Unable lookup items from secondary index");
        }

        [TestMethod]
        public void TestFindSecondaryFindMultipleWithSameIndex()
        {
            var items = this.CreateRandomBasicObjects(30);
            foreach (var item in items)
            {
                item.SecondaryIndex = items.Count;
            }
            _collection.Put(items);

            var foundItems = _collection.FindRange(p => p.SecondaryIndex, items.Count, items.Count);
            SortedSet<int> foundItemIds = new SortedSet<int>(foundItems.Select(i => i.Id));
            SortedSet<int> itemIds = new SortedSet<int>(items.Select(i => i.Id));

            Assert.IsTrue(foundItemIds.SetEquals(itemIds), "Unable to find all items");
        }
        
        [TestMethod]
        public void TestFindSecondaryFindMultipleWithRangeIndex()
        {
            var items = this.CreateRandomBasicObjects(30);
            foreach (var item in items)
            {
                item.SecondaryIndex = item.Id * 2;
            }
            _collection.Put(items);

            var foundItems = _collection.FindRange(p => p.SecondaryIndex, items[0].SecondaryIndex, items.Last().SecondaryIndex);
            SortedSet<int> foundItemIds = new SortedSet<int>(foundItems.Select(i => i.Id));
            SortedSet<int> itemIds = new SortedSet<int>(items.Select(i => i.Id));

            Assert.IsTrue(foundItemIds.SetEquals(itemIds), "Unable to find all items");
        }

        [TestMethod]
        public void TestFindSecondaryFindMultipleWithRangeIndexReverse()
        {
            var items = this.CreateRandomBasicObjects(30);
            foreach (var item in items)
            {
                item.SecondaryIndex = item.Id * 2;
            }
            _collection.Put(items);

            var foundItems = _collection.FindRange(p => p.SecondaryIndex, items.Last().SecondaryIndex, items[0].SecondaryIndex);
            SortedSet<int> foundItemIds = new SortedSet<int>(foundItems.Select(i => i.Id));
            SortedSet<int> itemIds = new SortedSet<int>(items.Select(i => i.Id));

            Assert.IsTrue(foundItemIds.SetEquals(itemIds), "Unable to find all items");
        }
        
        [TestMethod]
        public void TestFindSecondaryFindMultipleWithOpenRange()
        {
            var items = this.CreateRandomBasicObjects(30);
            foreach (var item in items)
            {
                item.SecondaryIndex = 100 + item.Id * 2;
            }
            _collection.Put(items);

            var foundItems = _collection.FindRange(p => p.SecondaryIndex, 0, 100000);
            SortedSet<int> foundItemIds = new SortedSet<int>(foundItems.Select(i => i.Id));
            SortedSet<int> itemIds = new SortedSet<int>(items.Select(i => i.Id));

            Assert.IsTrue(foundItemIds.SetEquals(itemIds), "Unable to find all items");
        }

        [TestMethod]
        public void TestDeleteSingleObject()
        {
            var item = new BasicObject { Id = 100 };
            _collection.Put(item);
            
            Assert.IsTrue(_collection.Delete(item), "Unable to delete single item");
            Assert.IsNull(_collection.Find(p => p.Id, 100), "Item was not totally deleted");
        }

        [TestMethod]
        public void TestDeleteMultipleObjects()
        {
            var items = this.CreateRandomBasicObjects(30);
            _collection.Put(items);

            Assert.IsTrue(_collection.Delete(items), "Unable to delete multiple items");
            Assert.IsFalse(
                _collection.FindMany(p => p.Id, items.Select(i => i.Id).ToList()).Any(i => i != null),
                "Some item was found when it should have been deleted");
        }

        [TestMethod]
        public void TestDeleteMultipleObjectsWithParams()
        {
            var items = this.CreateRandomBasicObjects(5);
            _collection.Put(items);

            Assert.IsTrue(_collection.Delete(items), "Unable to delete multiple items");
            Assert.IsFalse(
                _collection.FindMany(p => p.Id, 0, 1, 2, 3, 4).Any(i => i != null),
                "Some item was found when it should have been deleted");
        }

        [TestMethod]
        public void TestDeleteRemovesIndexEntries()
        {
            var item = new BasicObject { Id = 100, SecondaryIndex = 30 };
            _collection.Put(item);

            Assert.IsTrue(_collection.Delete(item), "Unable to delete single item");
            Assert.IsNull(_collection.Find(p => p.SecondaryIndex, 30), "Item index was not cleanedup");
        }

        private IList<BasicObject> CreateRandomBasicObjects(int count)
        {
            var lst = new List<BasicObject>();
            for (int i = 0; i < count; i++)
            {
                lst.Add(new BasicObject { Id = i });
            }

            return lst;
        }
    }
}
