//// The communication between the client and the server is done through
//// messages. These messages could be commands that indicate that something
//// needs to be done or they could be information for the result of the
//// commands or information (events).

import ream/storage/schema
import ream/storage/schema/data_type
import ream/storage/schema/table

/// The identifier used for the events. It's used for each stream. When we push
/// an event it's marked with a sequential number. These numbers cannot be
/// duplicated and the server is in charge of assigning them to the event when
/// it's created. The lowest number will be always 1.
pub type EventId =
  Int

/// The name for the streams should be an alphanumeric name of size between 3
/// and 20. No symbols or spaces are allowed.
pub type EventQueueName =
  String

/// The name assigned to the kind or type of aggregators. This name should be
/// an alphanumeric name of size between 3 and 20. Only underscore symbol is
/// allowed in addition to alphanumeric characters.
pub type Aggregator =
  String

/// The identifier used for the aggregator element to be stored. It's
/// recommended you use UUID but actually, you could use whatever while
/// it's identifying uniquely to the aggregation element.
pub type AggregateId =
  Int

/// The name assigned to a projection. This name should be an alphanumeric
/// of size between 3 and 20. Only underscore symbol is allowed in addition
/// to alphanumeric characters.
pub type ProjectionName =
  String

/// The name assigned to a field inside of a projection. This name should
/// be alphanumeric (underscore is also allowed) between 3 and 20.
pub type ProjectionFieldName =
  String

/// Index types for projections. Each projection let us define indexes and
/// they could be one of the following types.
pub type ProjectionIndex {
  /// Primary key index. It means it will be in use for locating and storing
  /// mainly the data.
  PrimaryKey

  /// Unique key index. Similar to the previous one but the storage of the
  /// element isn't depending on this one. Keeping the integrity, it needs
  /// to be check every time we perform a modification inside of the
  /// projection.
  UniqueKey

  /// Key index. Similar to the previous but it let add duplicated
  /// elements.
  Index

  /// No indexes. It's the default value for most of the fields. See
  /// `ProjectionField` for further information.
  NoIndex
}

/// Information about the field inside of a projection. The information
/// provided is the name and if there's available a index.
pub type ProjectionField {
  /// The projection field accepts two values:
  /// 
  /// - `name` the name of the projection, see `ProjectionFieldName`.
  /// - `index` the kind of the index (if any) the field has, you can see
  ///   the possible values in `ProjectionIndex`.
  ProjectionField(name: ProjectionFieldName, index: ProjectionIndex)
}

/// This is the list of commands or requests that could be triggered from the
/// client to the server or vice versa.
pub type Message {

  /// This could be received from/sent to the server. The interaction could be:
  /// 
  /// ```
  /// S: PING
  /// C: PONG
  /// ```
  /// 
  /// Or otherwise:
  /// 
  /// ```
  /// C: PING
  /// S: PONG
  /// ```
  /// 
  /// The server could be configured with the _keep-alive_ option and depending
  /// on the time, it could be sending a `PING` message every X seconds of
  /// inactivity. If the server isn't receiving the response from the client in
  /// a specified time (default 5 seconds) the server closes the connection.
  /// 
  /// The same logic could be implemented in the client. We could send a `PING`
  /// to the server and if the server isn't replying in a specific number of
  /// seconds, the client should close that connection.
  /// 
  /// > **Note**
  /// > It's intended that the connection should be blocked until we receive
  /// > the `PONG` response. That's good idea if we need perform a pause in the
  /// > communication because we detected a big pressure in the channel.
  Ping

  /// See `Ping` for futher information.
  Pong

  /// The information received from the server. We are receiving events when
  /// they are published and stored correctly in the persistent storage. The
  /// event is formed using the stream name, the event ID and size, and then
  /// the event content as a new line:
  /// 
  /// ```
  /// S: EVENT emails 1001 16
  /// S: {"name":"peter"}
  /// ```
  Event(EventQueueName, EventId, size: Int, content: BitString)

  /// The subscription is performed only from the client to the server. The
  /// subscription accepts the following parameters:
  /// 
  /// - `stream` the name to the stream to be subscribed.
  /// - `event-id` the ID of the event where we want to start to retrieve the
  ///   events.
  /// 
  /// > **Warning**
  /// > Be careful indicating a lower number for a stream very populated
  /// > because it could block the responses from the server until you receive
  /// > all of the events required.
  EventSubscribe(EventQueueName, EventId)

  /// The response to `EventSuscribe` request is a summary indicating the
  /// initial ID for the event we are going to receive the last ID for the last
  /// event we are going to receive and, of course, all of the events, one by
  /// one:
  /// 
  /// ```
  /// C: EVENT SUBSCRIBE emails 1
  /// S: EVENT SUBSCRIBED emails 1 1001
  /// S: ...
  /// S: EVENT emails 1001 16
  /// S: {"name":"peter"}
  /// ```
  /// 
  /// If there are no events inside of the stream at the moment, we will
  /// receive:
  /// 
  /// ```
  /// C: EVENT SUBSCRIBE removals 1
  /// S: EVENT SUBSCRIBED emails EMPTY
  /// ```
  /// 
  /// When we are using `EMPTY` the values for `first_event_id` and
  /// `last_event_id` are going to be both zero (0).
  EventSubscribed(
    EventQueueName,
    first_event_id: EventId,
    last_event_id: EventId,
  )

  /// The client could request the store of an event to the server. The
  /// publication of the event requires the name of the stream where the
  /// event is going to be published and the size of the event. Following
  /// that, the event is sent:
  /// 
  /// ```
  /// C: EVENT PUBLISH emails 14
  /// C: {"name":"tom"}
  /// S: EVENT PUBLISHED emails 1002
  /// ```
  /// 
  EventPublish(EventQueueName, size: Int, content: BitString)

  /// See `EventPublish`.
  EventPublished(EventQueueName, EventId)

  /// If the event couldn't be published then the response will be:
  /// 
  /// ```
  /// C: EVENT PUBLISH emails 14
  /// C: {"name":"peter"}
  /// S: EVENT NON PUBLISHED emails "Invalid message"
  /// ```
  EventNonPublished(EventQueueName, reason: String)

  /// It's removing the subscription for the current connection. It's performed
  /// by the client.
  /// 
  /// ```
  /// C: EVENT UNSUBSCRIBE emails
  /// S: EVENT UNSUBSCRIBED emails
  /// ```
  /// 
  /// > **Note**
  /// > The server should flush all of the events waiting to be sent to the
  /// > client when an unsubscribe request is performed.
  EventUnsubscribe(EventQueueName)

  /// See `EventUnsubscribe`.
  EventUnsubscribed(EventQueueName)

  /// The client could request the list of the streams available in the system.
  /// The response is a list of the streams:
  /// 
  /// ```
  /// C: EVENT LIST
  /// S: EVENT LISTED 2 emails users
  /// ```
  EventList

  /// See `EventList`.
  EventListed(List(EventQueueName))

  /// The client could request the removal of an event stream completely from
  /// the persistent storage:
  /// 
  /// ```
  /// C: EVENT REMOVE emails
  /// S: EVENT REMOVED emails
  /// ```
  /// 
  /// > **Warning**
  /// > The event removal is removing completely the event stream and all of
  /// > its content. It's not possible to recover the information. That's
  /// > because we could configure each stream for disabling this action. In
  /// > this case, we could receive a `EVENT NON REMOVED` response.
  EventRemove(EventQueueName)

  /// See `EventRemove`.
  EventRemoved(EventQueueName)

  /// The aggregation request is performed by the client and it stores the
  /// information from an aggregator into the persistent storage. The
  /// aggregator should be identified by a name and ID for the aggregator, the
  /// size of the aggregator content and then the content:
  /// 
  /// ```
  /// C: AGGREGATE SET emails "27d1b5a0-c54f-4664-a549-b876b0bb3661" 15
  /// C: {"emails":1002}
  /// S: AGGREGATE SET DONE emails "27d1b5a0-c54f-4664-a549-b876b0bb3661"
  /// ```
  /// 
  /// > **Warning**
  /// > The IDs here are related to UUID because aggregate works using those as
  /// > IDs. We could indicate them in two different ways: the string way which
  /// > is the hexadecimal representation with or without dashes and the
  /// > integer representation.
  AggregateSet(name: Aggregator, id: AggregateId, size: Int, content: BitString)

  /// See `AggregateSet`.
  AggregateSetDone(Aggregator, AggregateId)

  /// If there are errors presented the return will be as follows:
  /// 
  /// ```
  /// C: AGGREGATE SET emails "d0e72dec-6c77-4cb3-9fe1-7d86ab603a92" 12
  /// C: {"emails":1002}
  /// S: AGGREGATE NON SET emails "d0e72dec-6c77-4cb3-9fe1-7d86ab603a92" "Invalid message"
  /// ```
  AggregateNonSet(Aggregator, AggregateId, reason: String)

  /// It's similar to the `AGGREGATE SET` but it's not indicating the content,
  /// only the name and ID for the aggregation to be deleted. The response is a
  /// bit different:
  /// 
  /// ```
  /// C: AGGREGATE REMOVE emails "27d1b5a0-c54f-4664-a549-b876b0bb3661"
  /// S: AGGREGATE REMOVED emails "27d1b5a0-c54f-4664-a549-b876b0bb3661"
  /// ```
  /// 
  /// The action is always returning the correct state even when the object to
  /// be removed is not found.
  AggregateRemove(Aggregator, AggregateId)

  /// See `AggregateRemove`.
  AggregateRemoved(Aggregator, AggregateId)

  /// It's retrieving the aggregation from the persistent storage:
  /// 
  /// ```
  /// C: AGGREGATE GET emails "27d1b5a0-c54f-4664-a549-b876b0bb3661"
  /// S: AGGREGATE GOT emails "27d1b5a0-c54f-4664-a549-b876b0bb3661" 15
  /// S: {"emails":1002}
  /// ```
  /// 
  AggregateGet(Aggregator, AggregateId)

  /// See `AggregateGot`.
  AggregateGot(Aggregator, AggregateId, size: Int, content: BitString)

  /// If the aggregate isn't found the response is as follows:
  /// 
  /// ```
  /// C: AGGREGATE GET emails "27d1b5a0-c54f-4664-a549-b876b0bb3661"
  /// S: AGGREGATE NON GOT emails "27d1b5a0-c54f-4664-a549-b876b0bb3661" "Not found"
  /// ```
  AggregateNonGot(Aggregator, AggregateId, reason: String)

  /// It's retrieving the list of aggregators available in the system:
  /// 
  /// ```
  /// C: AGGREGATE LIST
  /// S: AGGREGATE LISTED 5 emails users accounts orders payments
  /// ```
  /// 
  /// The return gives the number of aggregators and the list of them.
  AggregateList

  /// See `AggregateList`.
  AggregateListed(List(Aggregator))

  /// The projection stores information processed after the event and it adds
  /// the information like with the aggregator but adds more indexes for
  /// searching the information based on different fields. Using this request
  /// we can create a projection with the following information:
  /// 
  /// ```
  /// C: PROJECTION CREATE users 7 +name +email position address city *country salary
  /// S: PROJECTION CREATED users
  /// ```
  /// 
  /// The plus (+) signs indicate the fields are unique keys and the star (*)
  /// signs indicate the fields are indexes. The response indicates the name of
  /// the projection created.
  ProjectionCreate(ProjectionName, List(ProjectionField))

  /// See `ProjectionCreated`.
  ProjectionCreated(ProjectionName)

  /// It's removing the projection from the persistent storage:
  /// 
  /// ```
  /// C: PROJECTION DROP users
  /// S: PROJECTION DROPPED users
  /// ```
  /// 
  /// The action is always returning the correct state even when the object to
  /// be removed is not found.
  ProjectionDrop(ProjectionName)

  /// See `ProjectionDrop`.
  ProjectionDropped(ProjectionName)

  /// The projection stores the information required in the persistent storage.
  /// The request is similar to the aggregator but it's adding more indexes for
  /// searching the information based on different fields. Using this request
  /// we can create a projection with the following information:
  /// 
  /// ```
  /// C: PROJECTION SET users 130
  /// C: {"name":"peter","email":"peter@mail.com","position":"developer","address":"street 1","city":"London","country":"UK","salary":1000}
  /// S: PROJECTION SET DONE users peter
  /// ```
  /// 
  /// The content must contain all of the fields indicated in the creation of
  /// the projection. The response indicates the name of the projection
  /// created.
  ProjectionSet(ProjectionName, size: Int, data: BitString)

  /// See `ProjectionSet`.
  ProjectionSetDone(ProjectionName, id: data_type.DataType)

  /// If there are errors presented the return will be as follows:
  /// 
  /// ```
  /// C: PROJECTION SET users 2
  /// C: {}
  /// S: PROJECTION NON SET users "Missing required fields"
  /// ```
  ProjectionNonSetDone(ProjectionName, reason: String)

  /// It retrieves the projection from the persistent storage based on the
  /// expression provided:
  /// 
  /// ```
  /// C: PROJECTION SELECT users 84
  /// C: city IN ("London","Madrid") AND position != "commercial" AND email =~ "@mail.com$"
  /// S: PROJECTION SELECTED users 132
  /// S: [{"name":"peter","email":"peter@mail.com","position":"developer","address":"street 1","city":"London","country":"UK","salary":1000}]
  /// ```
  /// 
  /// The expression sent to the server is based on the SQL syntax. The
  /// response is a list of the projections found. You can use different
  /// comparison operators for creating the expression:
  /// 
  /// - `=`, `!=`, `>`, `<` `>=` `<=` comparison operators
  /// - `AND`, `OR` logical operators
  /// - `IN` for checking if the value is inside of a list of values
  /// - `=~` for checking if the value is like the regular expression provided
  /// - `CONTAINS` is a simple way for strings to know if one is contained in
  ///   another.
  /// 
  /// > **Note**
  /// > The projection should have all of the information you need. If you
  /// > think you need something like JOINs then you should create a new
  /// > projection with the information you need.
  ProjectionSelect(
    ProjectionName,
    size: Int,
    query: BitString,
    conditions: schema.Operation,
  )

  /// See `ProjectionSelect`.
  ProjectionSelected(
    ProjectionName,
    size: Int,
    data: List(List(data_type.DataType)),
  )

  // TODO changes should be a list of field names and expressions
  /// It's updating the projection from the persistent storage based on the
  /// expression provided:
  /// 
  /// ```
  /// C: PROJECTION UPDATE users 27 22
  /// C: city IN ("London","Madrid")
  /// C: {"position":"manager"}
  /// S: PROJECTION UPDATED users 1
  /// ```
  ProjectionUpdate(
    ProjectionName,
    changes: table.DataSet,
    conditions: schema.Operation,
  )

  /// The return indicates the number of projections updated.
  /// In this case, we are sending two expressions, the first one is for
  /// selecting the projections to be updated and the second one is for
  /// updating the information. As an advantage, the JSON sent will be
  /// evaluated, so we can add expressions like:
  /// 
  /// ```
  /// C: PROJECTION UPDATE users 18 43
  /// C: city IN ("London")
  /// C: {"position":"manager","salary":salary+1000}
  /// S: PROJECTION UPDATED users 1
  /// ```
  ProjectionUpdated(ProjectionName, modified_rows: Int)

  /// It deletes the projection entries from the persistent storage based on
  /// the expression provided:
  /// 
  /// ```
  /// C: PROJECTION DELETE users 17
  /// C: city IN ("Miami")
  /// S: PROJECTION DELETED users 12
  /// ```
  /// 
  /// We are removing the projections based on the expression provided. The
  /// return indicates the number of projections deleted.
  ProjectionDelete(ProjectionName, conditions: schema.Operation)

  /// See `ProjectionDelete`.
  ProjectionDeleted(ProjectionName, removed_rows: Int)
}
