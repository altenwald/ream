import gleam/dynamic
import gleam/map.{Map}
import gleam/erlang/process
import gleam/otp/actor

pub type Message(e) {
  Shutdown
  Lookup(name: String, from: process.Subject(e))
  Register(String, process.Subject(dynamic.Dynamic))
  Unregister(String)
  ProcessDown(process.Pid, dynamic.Dynamic)
}

pub type Reply =
  Result(process.Subject(dynamic.Dynamic), Nil)

pub type Context {
  Context(
    names: Map(
      String,
      #(process.Subject(dynamic.Dynamic), process.ProcessMonitor),
    ),
    processes: Map(process.Pid, String),
  )
}

pub fn start() {
  let state = Context(names: map.new(), processes: map.new())
  let init = fn() { actor.Ready(state, process.new_selector()) }
  case actor.start_spec(actor.Spec(init, 5000, handle_message)) {
    Ok(subject) -> {
      persistent_term_put("ream_registry", subject)
      Ok(subject)
    }
    Error(error) -> Error(error)
  }
}

pub fn stop() {
  let registry = persistent_term_get("ream_registry")
  actor.send(registry, Shutdown)
}

pub fn is_alive() -> Bool {
  let registry = persistent_term_get("ream_registry")
  process.is_alive(process.subject_owner(registry))
}

pub fn register(name: String, subject: process.Subject(dynamic.Dynamic)) {
  let registry = persistent_term_get("ream_registry")
  actor.send(registry, Register(name, subject))
}

pub fn unregister(name: String) {
  let registry = persistent_term_get("ream_registry")
  actor.send(registry, Unregister(name))
}

pub fn lookup(name: String) -> Result(process.Subject(dynamic.Dynamic), Nil) {
  let registry = persistent_term_get("ream_registry")
  actor.call(registry, Lookup(name, _), 10)
}

@external(erlang, "persistent_term", "get")
pub fn persistent_term_get(key: String) -> process.Subject(e)

@external(erlang, "persistent_term", "put")
pub fn persistent_term_put(key: String, value: process.Subject(e)) -> Nil

pub fn handle_message(
  message: Message(Reply),
  context: Context,
) -> actor.Next(Message(Reply), Context) {
  case message {
    Shutdown -> {
      actor.Stop(process.Normal)
    }
    Lookup(name, reply_subject) -> {
      case map.get(context.names, name) {
        Error(Nil) -> {
          actor.send(reply_subject, Error(Nil))
          actor.continue(context)
        }
        Ok(#(subject, _monitor)) -> {
          actor.send(reply_subject, Ok(subject))
          actor.continue(context)
        }
      }
    }
    Register(name, subject) -> {
      let pid = process.subject_owner(subject)
      let monitor = process.monitor_process(pid)
      let names = map.insert(context.names, name, #(subject, monitor))
      let processes = map.insert(context.processes, pid, name)
      let selector =
        process.new_selector()
        |> process.selecting_process_down(
          monitor,
          fn(process_down: process.ProcessDown) {
            ProcessDown(process_down.pid, process_down.reason)
          },
        )
      Context(names: names, processes: processes)
      |> actor.continue()
      |> actor.with_selector(selector)
    }
    Unregister(name) -> {
      case map.get(context.names, name) {
        Error(Nil) -> {
          actor.continue(context)
        }
        Ok(#(subject, monitor)) -> {
          process.demonitor_process(monitor)
          let names = map.delete(context.names, name)
          let pid = process.subject_owner(subject)
          let processes = map.delete(context.processes, pid)
          // TODO check how to remove selector
          Context(names: names, processes: processes)
          |> actor.continue()
        }
      }
    }
    ProcessDown(pid, _reason) -> {
      case map.get(context.processes, pid) {
        Error(Nil) -> {
          actor.continue(context)
        }
        Ok(name) -> {
          let names = map.delete(context.names, name)
          let processes = map.delete(context.processes, pid)
          Context(names: names, processes: processes)
          |> actor.continue()
        }
      }
    }
  }
}
