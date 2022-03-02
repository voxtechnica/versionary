# Versionary

Versionary provides an opinionated way of managing **versioned entities** in a NoSQL database, such as
[AWS DynamoDB](https://aws.amazon.com/dynamodb/). It's a simple way of managing "wide rows", which
provide really fast access to denormalized data, answering specific questions that one might have
for the data. And, it insulates the developer from some details of the underlying NoSQL database.
However, if you're designing your table schema, you'll need to understand the basic concepts of
[partition and sort keys](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.Partitions.html).
Partition keys are used for grouping data, and sort keys are used for sorting data in the group.
They must be unique. Versionary benefits from using Time-based Unique Identifiers
([TUID](https://pkg.go.dev/github.com/voxtechnica/tuid-go)) for entities, because they contain an
embedded timestamp at the beginning of the ID, such that an alphabetical sort is also chronological.

One of the wide rows (the "EntityRow") is the complete revision history of an entity. This is a list
of all the versions of the entity, sorted chronologically. The partition key is the entity ID, and
the sort key is the version ID. If the entity is never revised (such as an event in an event log),
then there will only be one version, and the partition key and sort key will be the same.

There is also a collection of "IndexRows", which are typically lists of entities grouped by a
particular attribute value. These rows contain only the most recent version of each entity. An example
of an index row might be articles grouped by their author. The partition key would be the author ID,
and the sort key would be the article ID.

The entity row and index rows are all stored in a single table, reducing the number of separate
tables in the database. Versionary takes care to ensure that the index rows reflect current
versions of the entities. It also maintains lists of all the partition keys used for each row, so
that you can efficiently "walk the data" for all the entities, and so that you know what the complete
vocabulary is for all the values used for grouping things.

To save space in the denormalized database, the entity values are stored as compressed JSON. This helps,
but for large entities (such as articles), it can take up a lot of space. To avoid this, you can create
wide rows that store only the entity ID as a sort key, or possibly a combination of the entity ID and
an optional associated text or numeric value (e.g. the article ID and it's title). Then, you could use
a two-stage approach, where first you get the list of article IDs and titles for a given author, and if
you need the full body of the articles, you can fetch a collection of them by ID, in parallel, in a
second stage.

## Installation

Versionary requires Go 1.18 or later, because it takes advantage of Type Parameters ("Generics").

```bash
go get github.com/voxtechnica/versionary
```

To use Versionary and run its tests, you'll need an AWS account, and you'll need to configure your
workstation to use the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).
The integration test creates, exercises, and deletes a DynamoDB table. For testing in your applications,
you can use the provided MemTable implementation, which is backed by a simple in-memory table, and
supports the same TableReader, TableWriter, and TableReadWriter interfaces.
