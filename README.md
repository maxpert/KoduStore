# KoduStore

KoduStore makes marries objects and LevelDB UWP in a delightful way. It's a really basic document store with support for primary and secondary index support; this will solve most of your problems. Right now it PCL (for UWP). It has only two dependencies, [NewtonSoft JSON](https://www.nuget.org/packages/Newtonsoft.Json/) and [LevelDB for UWP](https://www.nuget.org/packages/LevelDB.UWP/1.18.0-beta).

# Features

 - Simple yet powerful API
 - Threadsafe - allows multiple readers and single writer 
 - Cleaner abstraction with documents
 - Ultra lightweight < 1K lines of code
 - Detailed testcase coverage

# Quick Tutorial

It's simple, you can define your storage object as plain C# objects like this:

```c#

public class Post
{
    [PrimaryIndex]
    public long Id { get; set; }
     
    [SecondaryIndex]
    public string Author { get; set; }
    
    [SecondaryIndex]
    public int Upvotes { get; set; }
    
    public string Title { get; set; }
    
    public string Body { get; set; }
    
    public DateTime Published { get; set; }
    
    public List<Comment> Comments { get; set; }
}


```

## Opening collection

To save a document you must open a collection and then put the document in collection:

```c#
    var collection = new Collection<Post>("posts");
    collection.Open(); // Opens posts for Post item type
    
    collection.Put(new Post {
        Id = 1,
        // ... more members here
    });
```

## Finding a saved document

Only fields marked with ```SecondaryIndex``` or ```PrimaryIndex``` attributes can be used to query documents. To retrieve a saved document from collection just call find (find has many flavours, we are just looking at one):

```c#
    Post post = collection.Query(i => i.Id).Get(1); // Query collection for post Id, value 1
```

## Delete a document

Given an item you can simply call delete on item to delete it from collection:

```c#
    collection.Delete(post); // Deletes document against post.Id
```

## Finding by document secondary indexes

Just like expression ```i => i.Id``` use a valid secondary index from your object:

```c#
    collection.Query(i => i.Author).Get("zohaib");
```

## Finding documents within a Range

Damn simple!

```c#
    IList<Post> posts = collection.QueryRange(i => i.Id).Get(1, 100);
    IList<Post> popularPosts = collection.QueryRange(i => i.Upvotes).Get(100000, 1);
```

The Get call takes two parameters starting key and ending key. Passing them in reverse order will iterate over keys in revers order hence the results would be sorted in reverse order.

## Finding documents from starting point to a limit

Basic yet powerful! All indexes are stored in an increasing sorted order, so you can start picking up items from particular point upto limit (or end of range).

```c#
    // 30 posts from Id >= 10
    IList<Post> posts = collection.QueryScan(i => i.Id)
                                  .Limit(30)
                                  .GetAll(10); 
```

This starts scanning records from Id == 10 and keeps cursor moving in forward direction until ```Limit``` is fulfilled or there are no more Id records to scan. If there is no Id == 10, results will start from first key >= 10. Scans can be done in reverse direction as well. 

```c#
    // Scanning from MAX_VOTE_COUNT backwards in increasing sorted index of Upvotes
    IList<Post> top10Posts = collection.QueryScan(i => i.Upvotes)
                                        .Backwards()
                                        .Limit(10)
                                        .GetAll(MAX_VOTE_COUNT); 
```

This scans for all the items with ```Upvotes``` <= MAX_VOTE_COUNT. Again it's quite possible there is no item with MAX_VOTE_COUNT, and just like before the scan will start from first item with heighest ```Upvotes``` in reverse direction.

# Roadmap
 - Association support
 - Manual filter and iteration support
 - Detailed documentation
 - Demo apps