//// The protocol to be connected to the database is based on plain TCP and no
//// credentials for the first version. The system is based on asynchronous
//// communication, when we are connected the system gives us the banner:
//// 
//// ```
//// REAM 1.0
//// ```
//// 
//// After that, we can send a command and the server sends us whatever
//// information. For example, if an event is published by another system, we
//// are receiving this:
//// 
//// ```
//// EVENT emails 1001 16
//// {"name":"peter"}
//// ```
//// 
//// That's telling us that we are receiving an event from the _emails_ stream,
//// with ID 1001 and size 15 bytes and then, in the following line the event.
//// We can also send a request like this:
//// 
//// ```
//// SUBSCRIBE emails 1
//// ```
//// 
//// Indicating we are going to subscribe to the _emails_ stream from ID 1.
//// 
//// We will see these commands from/to the server.
//// 
//// > **Warning**
//// > The connection must be pipelined. It means that a connection must be
//// > attending each request one by one. This should guarantee that the kind
//// > of response you are awaiting from the server is the response you need.

