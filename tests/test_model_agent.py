"""ModelAgent's loop, driven by a scripted stand-in for Ollama (no model or cluster needed)."""

from mini_s3.agents import ModelAgent


class FakeOps:
    def __init__(self):
        self.calls, self.finished = [], False

    def call(self, tool, **args):
        self.calls.append((tool, args))
        self.finished = tool == "done"
        return {"ok": True}


def scripted(*replies):
    seen = []

    def chat(messages):
        seen.append(list(messages))
        return replies[len(seen) - 1]
    return chat, seen


def call(name, **args):
    return {"function": {"name": name, "arguments": args}}


def test_runs_tool_calls_until_done_and_feeds_results_back():
    agent = ModelAgent("fake")
    agent._chat, seen = scripted(
        {"role": "assistant", "content": "", "tool_calls": [call("status")]},
        {"role": "assistant", "content": "", "tool_calls": [call("diagnose", fault="node_down", target="b"),
                                                             call("restart", target="b")]},
        {"role": "assistant", "content": "", "tool_calls": [call("done")]},
    )
    ops = FakeOps()
    agent.run(ops)
    assert [c[0] for c in ops.calls] == ["status", "diagnose", "restart", "done"]
    assert ops.calls[1][1] == {"fault": "node_down", "target": "b"}
    assert seen[1][-1] == {"role": "tool", "tool_name": "status", "content": '{"ok": true}'}


def test_string_arguments_are_parsed_and_chatter_gets_nudged():
    agent = ModelAgent("fake")
    agent._chat, seen = scripted(
        {"role": "assistant", "content": "I would check the status first."},
        {"role": "assistant", "content": "", "tool_calls": [
            {"function": {"name": "read_log", "arguments": '{"target": "a"}'}}]},
        {"role": "assistant", "content": "", "tool_calls": [call("done")]},
    )
    ops = FakeOps()
    agent.run(ops)
    assert ops.calls[0] == ("read_log", {"target": "a"})
    assert seen[1][-1]["content"].startswith("Use a tool")


def test_gives_up_after_repeated_chatter():
    agent = ModelAgent("fake")
    agent._chat = lambda messages: {"role": "assistant", "content": "Hmm."}
    ops = FakeOps()
    agent.run(ops)
    assert ops.calls == []
