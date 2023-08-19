import ream/storage/schema
import ream/storage/schema/data_type
import ream/storage/schema/table

pub type EventId =
  Int

pub type EventQueueName =
  String

pub type Aggregator =
  String

pub type AggregateId =
  Int

pub type ProjectionName =
  String

pub type ProjectionFieldName =
  String

pub type ProjectionIndex {
  PrimaryKey
  UniqueKey
  NoIndex
}

pub type ProjectionField {
  ProjectionField(name: ProjectionFieldName, index: ProjectionIndex)
}

pub type Json {
  Array(List(Json))
  False
  Null
  Number(Float)
  Object(List(#(String, Json)))
  String(String)
  True
}

pub type Message {
  Ping
  Pong
  Event(EventQueueName, EventId, Json)
  EventSubscribe(EventQueueName, EventId)
  EventSubscribed(
    EventQueueName,
    first_event_id: EventId,
    last_event_id: EventId,
  )
  EventPublish(EventQueueName, Json)
  EventPublished(EventQueueName, EventId)
  EventNonPublished(EventQueueName, reason: String)
  EventUnsubscribe(EventQueueName)
  EventUnsubscribed(EventQueueName)
  EventList
  EventListed(List(EventQueueName))
  EventRemove(EventQueueName)
  EventRemoved(EventQueueName)
  AggregateSet(Aggregator, AggregateId, content: BitString)
  AggregateSetDone(Aggregator, AggregateId)
  AggregateNonSet(Aggregator, AggregateId, reason: String)
  AggregateRemove(Aggregator, AggregateId)
  AggregateRemoved(Aggregator, AggregateId)
  AggregateGet(Aggregator, AggregateId)
  AggregateGot(Aggregator, AggregateId, content: BitString)
  AggregateNonGot(Aggregator, AggregateId, reason: String)
  AggregateList
  AggregateListed(List(Aggregator))
  ProjectionCreate(ProjectionName, List(ProjectionField))
  ProjectionCreated(ProjectionName)
  ProjectionDrop(ProjectionName)
  ProjectionDropped(ProjectionName)
  ProjectionSet(ProjectionName, data: BitString)
  ProjectionSetDone(ProjectionName, id: data_type.DataType)
  ProjectionSelect(ProjectionName, conditions: schema.Operation)
  ProjectionSelected(ProjectionName, data: List(List(data_type.DataType)))
  // TODO changes should be a list of field names and expressions
  ProjectionUpdate(
    ProjectionName,
    changes: table.DataSet,
    conditions: schema.Operation,
  )
  ProjectionUpdated(ProjectionName, modified_rows: Int)
  ProjectionDelete(ProjectionName, conditions: schema.Operation)
  ProjectionDeleted(ProjectionName, removed_rows: Int)
}
