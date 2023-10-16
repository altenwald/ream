import gleam/erlang/process
import ream/registry

pub fn start_and_stop_test() {
  let assert Ok(_subject) = registry.start()
  let assert True = registry.is_alive()
  registry.stop()
  process.sleep(100)
  let assert False = registry.is_alive()
}

pub fn register_test() {
  let assert Ok(_subject) = registry.start()

  let self = process.new_subject()
  let assert Error(Nil) = registry.lookup("test")
  registry.register("test", self)
  let assert Ok(pid) = registry.lookup("test")
  let assert True = pid == self

  registry.stop()
  process.sleep(100)
  let assert False = registry.is_alive()
}

pub fn unregister_test() {
  let assert Ok(_subject) = registry.start()

  let self = process.new_subject()
  registry.register("test", self)
  let assert Ok(_pid) = registry.lookup("test")
  registry.unregister("test")
  let assert Error(Nil) = registry.lookup("test")

  // ensure we can unregister a process that isn't registered
  registry.unregister("test")
  let assert Error(Nil) = registry.lookup("test")

  registry.stop()
  process.sleep(100)
  let assert False = registry.is_alive()
}

pub type Return(msg) {
  Return(process.Subject(msg))
}

pub fn process_down_test() {
  let assert Ok(_subject) = registry.start()

  let parent = process.new_subject()
  let multiselector =
    process.new_selector()
    |> process.selecting(parent, Return)
  let _process_pid =
    process.start(
      fn() {
        process.send(parent, process.new_subject())
        process.sleep(250)
      },
      False,
    )
  let assert Ok(Return(subject)) = process.select(multiselector, 5000)
  registry.register("test", subject)
  let assert Ok(_pid) = registry.lookup("test")
  process.sleep(250)
  let assert Error(Nil) = registry.lookup("test")

  registry.stop()
  process.sleep(100)
  let assert False = registry.is_alive()
}
