# Storage

Because we have three different ways to handle information we have to design three different ways to store or persist that information.

For example, the events have to be placed in a fast to append system with a index to localize fast each element based on the incremental ID provided for each event.

We are going to split these and explain these features for ensuring we are choosing a good method for storing them. But first we are going to talk about the files and how the information is going to be stored, after that we could go to the specifics for each of the ways we are handling information for.

## Storage

The content inside of the file is going to be in binary format based on the `term_to_binary/1` function from BEAM to store and retrieve that information as fast as possible but with the addition in the beginning of a 48-bits integer indicating the size of the element for helping us to navigate the elements if needed.

The files shouldn't be too huge, that's meaning we are needing to create as many files as possible and placing the information in these files. We need to different ways for storing information:

- raw information should be inside of the file and depending on the size we could configure a cache for keeping some of that information for a while.
- indexes is better to keep them inside of the memory but we have to talk about them independently because that's not the same in each element.

The organization of the files is going to be based on directories. The main idea is to use the UUID format, i.e. `276ea36d-02c9-4ea0-996a-72f4162a9fc3` is split in 5 chunks. When we are creating a file we are going to generate a new UUID and based on the data it will be replacing the `-` for `/`, then the path for that file should be `276ea36d/02c9/4ea0/996a/72f4162a9fc3` where the root directory will be `276ea36d` and the name of the file under the last directory is going to be `72f4162a9fc3`.

The index for the files is going to store the files opened putting the UUID in its binary format and based on the position inside of the file it means if that's considered the number 1, 2, 3, and so on.

## Schemas

Indeed we have only schemas for projections and the rest of the systems are only storing information about:

- `name` for the stream of events, collection of similar aggregators or tables for projections.
- `type` where we could say: `stream`, `aggregator`, or `projection`.

The files for these elements are going to be created under a directory with that name: `type/name`; i.e. if we are creating the stream `users` then the files for storing the events for that stream will be placed under `stream/users`.

In the case of the projections we are placing an extra file inside of that directory called `schema`. The file is containing a list of the fields available for the projection the content for each field is:

- `name` of the field.
- `type` of the filed. It could be one of these: `normal`, `index`, `unique`.

This way we have all of the information about the information we could find about the projection and the information to generate the indexes.

## Event Sourcing

The event sourcing has the following actions:

- Append a new event to the corresponding stream.
- Retrieve a list of events based on the correlative ID.

That said, it's clear that if we are not needing to modify that data we could create files where that information could be placed with a different limits, we need to create a new file:

- based on date, or
- based on the size of the file, or
- based on the number of elements inside of the file.

The first one is not a good fit because if one stream isn't used only on specific days it could mean there could be empty files and very populated files. The second one looks good but it could be happening that the each event could be big enough to overflow the limit and we get a file per event. The third one is a bit risky as well in the same case of the size, if the events are big and the number is too high it could mean the file could be huge and difficult to navigate.

I think the combination of the second of the third could give us a good solution. I mean, we could define a minimum limit for the file and if that limit is reached then one condition is achieved. The second condition should be a minimum of elements in terms of getting that number of elements even if the limit of the file was reached.

We could implement a much more flexible ways for handling the files but for the first version I think that the configuration for:

- `events.min_items_per_file` using a default value of 1000.
- `events.min_file_size` using a default value of 100M.

Then we are going to create files based on these parameters, and we are going to be appending that information inside of the files. The index for the event sourcing is going to be another file where we are going to be appending an 64-bit integer where the first 16-bits are going to be used to identified the file and the rest 48-bits are going to be in use to store the size where the event is starting. That said, we just defined some limits:

- The maximum size for an event is 281,474,976,710,656 (2^48) or 256TB. But I don't recommend to use a size higher than 1MB for an event even less if we are going to trigger thousands of events per second.
- The maximum number of files kept in the system will be 65,536 (2^16).

If we are sending events of 1KB based on the default configuration that's meaning we could store 100,000 events for each file or a total of 6,553,600,000 events using 6TB approx.

The index for events is going to be useful when we need to retrieve an event based on the correlative ID. Given the number we read that position and retrieve the information about which file we should to ask and the size where that event is starting. The event storage will provide us the size of the event and the content for the event.

## Aggregations

The aggregators are storing their information based on a key and a value, a document. It's clear this content is going to be updated and read more than inserted as a new element. However, I think the best approach is a copy-on-write strategy. If we need to modify the aggregator we insert a new element inside of the files in the same way we do with the events and then we update the index.

For avoiding some race-conditions that could arise reading the aggregator when we are processing an update the process in charge of the modification could lock the possible reading actions until the new element is updated inside of the index. However, there's situations where an eventual consistency is ok. We could implement a configuration parameter:

- `aggregator.read_lock_when_writing` using a default value of `true`, we encourage the consistency.

> **Note**
> However, it's not going to lock the events and it's not going to lock other aggregators or projections.

The process file has the information about the elements it's storing and if all of them are marked as deleted, then the file is removed. Based on that the information of the aggregators could be generated again from the events makes no sense to keep that information.

As another option, we could add a frequency to perform a data vacuum. That's going to be the process to get the information still valid from the older files where there's removed information and we put that information as if that's was called to be modified. It's going into the new files and then the old file is removed. We could define:

- `aggregator.vacuum_factor` where we specify a number between 1 and 100 and if that percentage of elements are removed then the file is removed and pushed all of their elements to a new file. The default value is 100.
- `aggregator.vacuum_frequency` indicate the time in seconds when the vacuum is triggered. The default value is 86400 (one day).

## Projections

The projections are a bit different because they have indexes a bit more complex and the requests could filter the information to be retrieved based on an expression provided. That's because each file is going to process their own indexes. This is great because we can parallelize the requests for reading and writing but it makes a bit more difficult the unique indexes.

About the indexes we have:

- unique indexes are keeping a record per index across all of the files.
- indexes are keeping many records per index. There will be one index per file.

The only one problem is about writing if we have defined unique keys. This is going to have a simplified version out of the files for assigning the unique index or refusing it, we are going to use a Cuckoo filter for ensuring the index wasn't inside of any of the files.

The storage of the projections is going to take place as the aggregations and in the same way we will have the vacuum of the information with the following parameters:

- `projector.vacuum_factor` where we specify a number between 1 and 100 and if that percentage of elements are removed then the file is removed and pushed all of their elements to a new file. The default value is 100.
- `projector.vacuum_frequency` indicate the time in seconds when the vacuum is triggered. The default value is 86400 (one day).
