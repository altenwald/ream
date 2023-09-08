//// Because we have three different ways to handle information we have to
//// design three different ways to store or persist that information.
//// 
//// For example, the events have to be placed fast to append with an index to
//// localize each element based on the incremental ID provided for each event.
//// 
//// We are going to split these and explain these features to ensure we are
//// choosing a good method for storing them. But first, we are going to talk
//// about the files and how the information is going to be stored. After that,
//// we could go to the specifics for each of the ways we are handling
//// information.
//// 
//// ## Storage
//// 
//// The content inside of the file is going to be in the binary format but the
//// storage layer isn't going to handle the way the layers are going to
//// convert the content data to be stored. The only requirement is to obtain a
//// BitString data to be stored.
//// 
//// In the file we are going to add size numbers at the beginning of the data
//// it could be a 16 bit, 32 bit, or 48 bit integer.
//// 
//// The files shouldn't be too huge, which means we need to create as many
//// files as possible and place the information in these files. We need
//// different ways of storing information:
//// 
//// - raw information should be inside of the file and depending on the size
////   we could configure a cache for keeping some of that information for a
////   while.
//// - indexes is better to keep them inside of the memory but we have to talk
////   about them independently because that's not the same in each element.
//// 
//// > **Warning**
//// > We will decide the size we are assigning for each file created. If we
//// > are handling different small files, it could require more resources from
//// > the point of view of descriptors (opened files) for the operating system
//// > (OS) but it could be faster if we could parallelize the reading/writing
//// > for each file.
//// 
//// The organization of the files is going to be based on directories. The
//// main idea is to use the UUID format, i.e.
//// `276ea36d-02c9-4ea0-996a-72f4162a9fc3` is split into 5 chunks. When we are
//// creating a file we are going to generate a new UUID and based on the data
//// it will be replacing the `-` for `/`, then the path for that file should
//// be `276ea36d/02c9/4ea0/996a/72f4162a9fc3` where the root directory will be
//// `276ea36d` and the name of the file under the last directory is going to
//// be `72f4162a9fc3`.
//// 
//// The index for the files is going to store the files opened putting the
//// UUID in its binary format and based on the position inside of the file it
//// means if that's considered the number 1, 2, 3, and so on.
//// 
//// You can get specific information about storage from:
//// - [Streams](stream)
//// - [KeyValue or KV](kv)
//// - [Schemas](schema)

