# KoduStore

KoduStore makes marries objects and LevelDB UWP in a delightful way. It's a really basic document store with support for primary and secondary index support. Right now it PCL (for UWP). It has only two dependencies, [NewtonSoft JSON](https://www.nuget.org/packages/Newtonsoft.Json/) and [LevelDB for UWP](https://visualstudiogallery.msdn.microsoft.com/4466a14f-49d7-4440-91e0-dd82d29d683a).
 
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
    Post post = collection.Find(i => i.Id, 1); // Query collection for post Id, value 1
```

## Delete a document

Given an item you can simply call delete on item to delete it from collection:

```c#
    collection.Delete(post); // Deletes document against post.Id
```

## Finding by secondary indexes

Just like expression ```i => i.Id``` use a valid secondary index from your object:

```c#
    collection.Find(i => i.Author, "zohaib");
```

## Finding by Range

Damn simple!

```c#
    IList<Post> posts = collection.FindRange(i => i.Id, 1, 100);
    IList<Post> popularPosts = collection.FindRange(i => i.Upvotes, 10, 1000000);
```

## Finding from starting point to a limit

Basic yet powerful! All indexes are stored in an increasing sorted order, so you can start picking up items from particular point upto limit (or end of range).

```c#
    IList<Post> posts = collection.FindFrom(i => i.Id, 10, limit: 30); // 30 posts from Id >= 10
    
    // Scanning from MAX_VOTE_COUNT backwards in increasing sorted index of Upvotes
    IList<Post> top10Posts = collection.FindFrom(i => i.Upvotes, MAX_VOTE_COUNT, Collection<Post>.ScanDirection.Backward, 10); 
```

# More to come!

 - Detailed tutorial
 - Documentation
 - More test coverage
