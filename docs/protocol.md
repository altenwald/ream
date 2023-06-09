# Protocol

The protocol to be connected to the database is based on plain TCP and no credentials for the first version. The system is based on asynchronous communication, when we are connected the system is giving us the banner:

```
REAM 1.0
```

After that, we can send whenever a command and the server is sending us whenever information. For example, if an event is published by another system, we are receiving this:

```
EVENT emails 1001 16
{"name":"peter"}
```

That's telling us that we are receiving an event from the _emails_ stream, with ID 1001 and size 15 bytes and then, in the following line the event. We can also send a request like this:

```
SUBSCRIBE emails 1
```

Indicating we are going to subscribe to the _emails_ stream from ID 1.

We will see these commands from/to the server.

> **Warning**
> The connection must be pipelined. It means that a connection must be attending each request one by one. This should guarantee that the kind of response you are awaiting from the server is the response you need.

## Concepts and types

We are going to use the following concepts and types in the protocol:

- **stream name**: The name for the streams should be an alphanumeric name of size between 3 and 20. No symbols or spaces are allowed.

- **event id**: The identifier used for the events. It's used for each stream. When we push an event it's marked with a sequential number. These numbers cannot be duplicated and the server is in charge to assigned them to the event when it's created. The lowest number will be always 1.

- **event**: The content of the event must be a valid JSON, an object that we could filter based on any of the root keys provided.

- **aggregator id**: The identifier used for the aggregator element to be stored. It's recommended you use UUID but actually, you could use whatever while it's identifying uniquely to the aggregation element.

- **aggregator name**: The name assigned to the kind or type of aggregators. This name should be an alphanumeric name of size between 3 and 20. No symbols or spaces are allowed.

- **aggregator content**: The information to be stored for the aggregator. It should be a valid JSON object.

## Commands or Requests

This is the list of commands or requests that could be triggered from the client to the server or vice versa.

### PING

This could be received from/sent to the server. The interaction could be:

```
S: PING
C: PONG
```

Or otherwise:

```
C: PING
S: PONG
```

The server could be configured with the _keep-alive_ option and depending on the time, it could be sending a `PING` message every X seconds of inactivity. If the server isn't receiving the response from the client in a specified time (default 5 seconds) the server closes the connection.

The same logic could be implemented in the client. We could send a `PING` to the server and if the server isn't replying in a specific number of seconds, the client should close that connection.

> **Note**
> It's intended that the connection should be blocked until we receive the `PONG` response. That's good idea if we need perform a pause in the communication because we detected a big pressure in the channel.

### EVENT

The information received from the server. We are receiving events when they are published and stored correctly in the persistent storage. The event is formed using the stream name, the event ID and size, and then the event content as a new line:

```
S: EVENT emails 1001 15
S: {"name":"peter"}
```

### EVENT SUBSCRIBE

The subscription is performed only from the client to the server. The subscription is accepting the following parameters:

- `stream` the name to the stream to be subscribed.
- `event-id` the ID of the event where we want to start to retrieve the events.

> **Warning**
> Be careful indicating a lower number for a stream very populated because it could block the responses from the server until you receive all of the events required.

The response to this request is a summary indicating the initial ID for the event we are going to receive and the last ID for the last event we are going to receive and, of course, all of the events, one by one:

```
C: EVENT SUBSCRIBE emails 1
S: EVENT SUBSCRIBED emails 1 1001
S: ...
S: EVENT emails 1001 15
S: {"name":"peter"}
```

If there are no events inside of the stream at the moment, we will receive:

```
C: EVENT SUBSCRIBE removals 1
S: EVENT SUBSCRIBED emails EMPTY
```

### EVENT PUBLISH

The client could request the store of an event to the server. The publication of the event requires the name of the stream where the event is going to be published and the size of the event. Followed to that, the event is sent:

```
C: EVENT PUBLISH emails 14
C: {"name":"tom"}
S: EVENT PUBLISHED emails 1002
```

If the event couldn't be published then the response will be:

```
C: EVENT PUBLISH emails 14
C: {"name":"peter"}
S: EVENT NON PUBLISHED emails "Invalid JSON"
```

### EVENT UNSUBSCRIBE

It's removing the subscription for the current connection. It's performed by the client.

```
C: EVENT UNSUBSCRIBE emails
S: EVENT UNSUBSCRIBED emails
```

> **Note**
> The server should flush all of the events waiting to be sent to the client when an unsubscribe request is performed.

### EVENT LIST

The client could request the list of the streams available in the system. The response is a list of the streams:

```
C: EVENT LIST
S: EVENT LISTED 2 emails users
```

### EVENT REMOVE

The client could request the removal of an event stream completely from the persistent storage:

```
C: EVENT REMOVE emails 1001
S: EVENT REMOVED emails 1001
```

> **Warning**
> The event removal is removing completely the event stream and all of its content. It's not possible to recover the information. That's because we could configure each stream for disabling this action. In this case, we could receive a `EVENT NON REMOVED` response.

### AGGREGATE SET

The aggregation request is performed by the client and it's storing the information from an aggregator into the persistent storage. The aggregator should be identified by a name and ID for the aggregator, the size of the aggregator content and then the content: 

```
C: AGGREGATE SET emails 27d1b5a0-c54f-4664-a549-b876b0bb3661 15
C: {"emails":1002}
S: AGGREGATE SET emails 27d1b5a0-c54f-4664-a549-b876b0bb3661
```

If there are errors presented the return will be as follows:

```
C: AGGREGATE SET emails 1 12
C: {"emails":1002}
S: AGGREGATE NON SET emails 1 "Invalid JSON"
```

### AGGREGATE REMOVE

It's similar to `AGGREGATE SET` but it's not indicating the content, only the name and ID for the aggregation to be deleted. The response is a bit different:

```
C: AGGREGATE REMOVE emails 27d1b5a0-c54f-4664-a549-b876b0bb3661
S: AGGREGATE REMOVED emails 27d1b5a0-c54f-4664-a549-b876b0bb3661
```

The action is always returning the correct state even when the object to be removed is not found.

### AGGREGATE GET

It's retrieving the aggregation from the persistent storage:

```
C: AGGREGATE GET emails 27d1b5a0-c54f-4664-a549-b876b0bb3661
S: AGGREGATE GOT emails 27d1b5a0-c54f-4664-a549-b876b0bb3661 15
S: {"emails":1002}
```

If the aggregate isn't found the response is as follows:

```
C: AGGREGATE GET emails 1
S: AGGREGATE NON GOT emails 1 "Not found"
```

### AGGREGATE LIST

It's retrieving the list of aggregators available in the system:

```
C: AGGREGATE LIST
S: AGGREGATE LISTED 5 emails users accounts orders payments
```

The return is giving the number of aggregators and the list of them.

### PROJECTION CREATE

The projection stores information processed after the event and it's adding the information like with the aggregator but adds more indexes for searching the information based on different fields. Using this request we can create a projection with the following information:

```
C: PROJECTION CREATE users 6 +name +email position address city *country salary
S: PROJECTION CREATED users
```

The plus (+) signs are indicating the fields are unique keys and the star (*) signs are indicating the fields are indexes. The response is indicating the name of the projection created.

### PROJECTION DROP

It's removing the projection from the persistent storage:

```
C: PROJECTION DROP users
S: PROJECTION DROPPED users
```

The action is always returning the correct state even when the object to be removed is not found.

### PROJECTION SET

The projection stores the information required in the persistent storage. The request is similar to the aggregator but it's adding more indexes for searching the information based on different fields. Using this request we can create a projection with the following information:

```
C: PROJECTION SET users 130
C: {"name":"peter","email":"peter@mail.com","position":"developer","address":"street 1","city":"London","country":"UK","salary":1000}
S: PROJECTION SET users peter
```

The content must contain all of the fields indicated in the creation of the projection. The response is indicating the name of the projection created.

If there are errors presented the return will be as follows:

```
C: PROJECTION SET users 2
C: {}
S: PROJECTION NON SET users "Missing required fields"
```

### PROJECTION SELECT

It's retrieving the projection from the persistent storage based on the expression provided:

```
C: PROJECTION SELECT users 84
C: city IN ("London","Madrid") AND position != "commercial" AND email =~ "@mail.com$"
S: PROJECTION SELECTED users 132
S: [{"name":"peter","email":"peter@mail.com","position":"developer","address":"street 1","city":"London","country":"UK","salary":1000}]
```

The expression sent to the server is based on the SQL syntax. The response is a list of the projections found. You can use different comparison operators for creating the expression:

- `=`, `!=`, `>`, `<` `>=` `<=`: comparison operators
- `AND`, `OR`: logical operators
- `IN`: for checking if the value is inside of a list of values
- `=~`: for checking if the value is like the regular expression provided

> **Note**
> The projection should have all of the information you need. If you think you need something like JOINs then you should create a new projection with the information you need.

### PROJECTION UPDATE

It's updating the projection from the persistent storage based on the expression provided:

```
C: PROJECTION UPDATE users 27 22
C: city IN ("London","Madrid")
C: {"position":"manager"}
S: PROJECTION UPDATED users 1
```

The return is indicating the number of projections updated.

In this case, we are sending two expressions, the first one is for selecting the projections to be updated and the second one is for updating the information. As an advantage, the JSON sent will be evaluated, so we can add expressions like:

```
C: PROJECTION UPDATE users 18 43
C: city IN ("London")
C: {"position":"manager","salary":salary+1000}
S: PROJECTION UPDATED users 1
```

### PROJECTION DELETE

it's deleting the projection entries from the persistent storage based on the expression provided:

```
C: PROJECTION DELETE users 17
C: city IN ("Miami")
S: PROJECTION DELETED users 12
```

We are removing the projections based on the expression provided. The return is indicating the number of projections deleted.
