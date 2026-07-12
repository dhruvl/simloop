from simloop._trace import TraceRecorder


def test_events_are_recorded_in_order() -> None:
    recorder = TraceRecorder()
    recorder.record("schedule", 0.0, 0, "f")
    recorder.record("run", 0.0, 0, "f")
    assert [event.kind for event in recorder.events] == ["schedule", "run"]
    assert recorder.events[0].seq == 0
    assert recorder.events[0].label == "f"


def test_identical_sequences_hash_identically() -> None:
    first, second = TraceRecorder(), TraceRecorder()
    for recorder in (first, second):
        recorder.record("schedule", 0.0, 0, "f")
        recorder.record("advance", 1.5, -1, "")
        recorder.record("run", 1.5, 0, "f")
    assert first.hash() == second.hash()
    assert len(first.hash()) == 64


def test_different_order_hashes_differently() -> None:
    first, second = TraceRecorder(), TraceRecorder()
    first.record("run", 0.0, 0, "f")
    first.record("run", 0.0, 1, "g")
    second.record("run", 0.0, 1, "g")
    second.record("run", 0.0, 0, "f")
    assert first.hash() != second.hash()


def test_cancel_events_change_the_hash() -> None:
    first, second = TraceRecorder(), TraceRecorder()
    first.record("run", 0.0, 0, "f")
    second.record("run", 0.0, 0, "f")
    second.record("cancel", 0.0, 1, "g")
    assert first.hash() != second.hash()
