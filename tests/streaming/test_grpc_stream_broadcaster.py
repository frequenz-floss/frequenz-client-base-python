# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for GrpcStreamBroadcaster class."""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack
from datetime import timedelta
from unittest import mock

import grpc
import grpc.aio
import pytest
from frequenz.channels import Receiver

from frequenz.client.base import retry, streaming
from frequenz.client.base.streaming import (
    StreamEvent,
    StreamFatalError,
    StreamRetrying,
    StreamStarted,
)


def _transformer(x: int) -> str:
    """Mock transformer."""
    return f"transformed_{x}"


@pytest.fixture
def receiver_ready_event() -> asyncio.Event:
    """Fixture for receiver ready event."""
    return asyncio.Event()


@pytest.fixture
def no_retry() -> mock.MagicMock:
    """Fixture for mocked, non-retrying retry strategy."""
    mock_retry = mock.MagicMock(spec=retry.Strategy)
    mock_retry.next_interval.return_value = None
    mock_retry.copy.return_value = mock_retry
    mock_retry.get_progress.return_value = "mock progress"
    return mock_retry


def make_error() -> grpc.aio.AioRpcError:
    """Mock error for testing."""
    return grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=grpc.aio.Metadata(),
        trailing_metadata=grpc.aio.Metadata(),
        details="mock details",
        debug_error_string="mock debug_error_string",
    )


def unary_stream_call_mock(
    name: str, side_effect: Callable[[], AsyncIterator[object]]
) -> mock.MagicMock:
    """Create a new mocked unary stream call."""
    # Sadly we can't use spec here because grpc.aio.UnaryStreamCall seems to be
    # dynamic and mock doesn't find `__aiter__` in it when creating the spec.
    call_mock = mock.MagicMock(name=name)
    call_mock.__aiter__.side_effect = side_effect
    call_mock.initial_metadata = mock.AsyncMock()
    return call_mock


@pytest.fixture
async def ok_helper(
    no_retry: mock.MagicMock,  # pylint: disable=redefined-outer-name
    receiver_ready_event: asyncio.Event,  # pylint: disable=redefined-outer-name
    retry_on_exhausted_stream: bool,
) -> AsyncIterator[streaming.GrpcStreamBroadcaster[int, str]]:
    """Fixture for GrpcStreamBroadcaster."""

    async def asynciter() -> AsyncIterator[int]:
        """Mock async iterator."""
        await receiver_ready_event.wait()
        for i in range(5):
            yield i
            await asyncio.sleep(0)  # Yield control to the event loop

    rpc_mock = mock.MagicMock(
        name="ok_helper_method",
        side_effect=lambda: unary_stream_call_mock(
            "ok_helper_unary_stream_call", asynciter
        ),
    )
    helper = streaming.GrpcStreamBroadcaster(
        stream_name="test_helper",
        stream_method=rpc_mock,
        transform=_transformer,
        retry_strategy=no_retry,
        retry_on_exhausted_stream=retry_on_exhausted_stream,
    )
    yield helper
    await helper.stop()


async def _split_message(
    receiver: Receiver[StreamEvent | str],
) -> tuple[list[str], list[StreamEvent]]:
    """Split the items received from the receiver into items and messages.

    Args:
        receiver: The receiver to process.

    Returns:
        A tuple containing a list of transformed items and a list of messages.
    """
    items: list[str] = []
    events: list[StreamEvent] = []
    async for item in receiver:
        match item:
            case StreamStarted() | StreamRetrying() | StreamFatalError():
                events.append(item)
            case str():
                items.append(item)
    return items, events


class _ErroringAsyncIter(AsyncIterator[int]):
    """Async iterator that raises an error after a certain number of successes."""

    def __init__(
        self, error: Exception, ready_event: asyncio.Event, *, num_successes: int = 0
    ):
        self._error = error
        self._ready_event = ready_event
        self._num_successes = num_successes
        self._current = -1

    async def __anext__(self) -> int:
        self._current += 1
        await self._ready_event.wait()
        if self._current >= self._num_successes:
            raise self._error
        return self._current

    async def initial_metadata(self) -> None:
        """Mock initial metadata method."""
        if self._current >= self._num_successes:
            raise self._error


def erroring_rpc_mock(
    error: Exception,
    ready_event: asyncio.Event,
    *,
    num_successes: int = 0,
    should_error_on_initial_metadata_too: bool = False,
) -> mock.MagicMock:
    """Fixture for mocked erroring rpc."""
    # In this case we want to keep the state of the erroring call
    erroring_iter = _ErroringAsyncIter(error, ready_event, num_successes=num_successes)
    call_mock = unary_stream_call_mock(
        "erroring_unary_stream_call", lambda: erroring_iter
    )
    if should_error_on_initial_metadata_too:
        call_mock.initial_metadata.side_effect = erroring_iter.initial_metadata
    rpc_mock = mock.MagicMock(name="erroring_rpc", return_value=call_mock)

    return rpc_mock


@pytest.mark.parametrize("retry_on_exhausted_stream", [True])
async def test_streaming_success_retry_on_exhausted(
    ok_helper: streaming.GrpcStreamBroadcaster[
        int, str
    ],  # pylint: disable=redefined-outer-name
    no_retry: mock.MagicMock,  # pylint: disable=redefined-outer-name
    receiver_ready_event: asyncio.Event,  # pylint: disable=redefined-outer-name
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test streaming success."""
    caplog.set_level(logging.INFO)
    items: list[str] = []
    events: list[StreamEvent] = []
    async with asyncio.timeout(1):
        receiver = ok_helper.new_receiver()
        receiver_ready_event.set()
        items, events = await _split_message(receiver)

    no_retry.next_interval.assert_called_once_with()
    assert items == [
        "transformed_0",
        "transformed_1",
        "transformed_2",
        "transformed_3",
        "transformed_4",
    ]
    assert events == []

    assert caplog.record_tuples == [
        (
            "frequenz.client.base.streaming",
            logging.ERROR,
            "test_helper: connection ended, retry limit exceeded (mock progress), "
            "giving up. Stream exhausted.",
        )
    ]


@pytest.mark.parametrize("retry_on_exhausted_stream", [False])
async def test_streaming_success(
    ok_helper: streaming.GrpcStreamBroadcaster[
        int, str
    ],  # pylint: disable=redefined-outer-name
    no_retry: mock.MagicMock,  # pylint: disable=redefined-outer-name
    receiver_ready_event: asyncio.Event,  # pylint: disable=redefined-outer-name
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test streaming success."""
    caplog.set_level(logging.INFO)
    items: list[str] = []
    events: list[StreamEvent] = []

    async with asyncio.timeout(1):
        receiver = ok_helper.new_receiver()
        receiver_ready_event.set()
        items, events = await _split_message(receiver)

    no_retry.next_interval.assert_not_called()

    assert items == [
        "transformed_0",
        "transformed_1",
        "transformed_2",
        "transformed_3",
        "transformed_4",
    ]
    assert events == []
    assert caplog.record_tuples == [
        (
            "frequenz.client.base.streaming",
            logging.INFO,
            "test_helper: connection closed, stream exhausted",
        )
    ]


@pytest.mark.parametrize("successes", [0, 1, 5])
async def test_streaming_error(  # pylint: disable=too-many-arguments
    successes: int,
    no_retry: mock.MagicMock,  # pylint: disable=redefined-outer-name
    receiver_ready_event: asyncio.Event,  # pylint: disable=redefined-outer-name
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test streaming errors."""
    caplog.set_level(logging.INFO)

    error = make_error()

    helper = streaming.GrpcStreamBroadcaster(
        stream_name="test_helper",
        stream_method=erroring_rpc_mock(
            error, receiver_ready_event, num_successes=successes
        ),
        transform=_transformer,
        retry_strategy=no_retry,
    )

    items: list[str] = []
    async with AsyncExitStack() as stack:
        stack.push_async_callback(helper.stop)

        receiver = helper.new_receiver()
        receiver_ready_event.set()
        items, _ = await _split_message(receiver)

    no_retry.next_interval.assert_called_once_with()
    assert items == [f"transformed_{i}" for i in range(successes)]
    assert caplog.record_tuples == [
        (
            "frequenz.client.base.streaming",
            logging.INFO,
            "test_helper: starting to stream",
        ),
        (
            "frequenz.client.base.streaming",
            logging.ERROR,
            "test_helper: connection ended, retry limit exceeded (mock progress), "
            f"giving up. Error: {error}.",
        ),
        (
            "frequenz.client.base.streaming",
            logging.INFO,
            "test_helper: stopping the stream",
        ),
    ]


async def test_streaming_transform_error(  # pylint: disable=too-many-arguments
    no_retry: mock.MagicMock,  # pylint: disable=redefined-outer-name
    receiver_ready_event: asyncio.Event,  # pylint: disable=redefined-outer-name
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test streaming transform errors."""
    caplog.set_level(logging.INFO)

    def transform_err(x: int) -> str:
        """Mock transform with err for odd values."""
        if x % 2 == 1:
            raise ValueError("No, you transform.")
        return f"transformed_{x}"

    async def asynciter() -> AsyncIterator[int]:
        """Mock async iterator."""
        await receiver_ready_event.wait()
        for i in range(5):
            yield i
            await asyncio.sleep(0)  # Yield control to the event loop

    rpc_mock = mock.MagicMock(
        name="ok_helper_method",
        side_effect=lambda: unary_stream_call_mock(
            "ok_helper_unary_stream_call", asynciter
        ),
    )

    helper = streaming.GrpcStreamBroadcaster(
        stream_name="test_helper",
        stream_method=rpc_mock,
        transform=transform_err,
        retry_strategy=no_retry,
    )

    items: list[str] = []
    async with AsyncExitStack() as stack:
        stack.push_async_callback(helper.stop)

        receiver = helper.new_receiver()
        receiver_ready_event.set()
        items, _ = await _split_message(receiver)

    assert items == [
        "transformed_0",
        "transformed_2",
        "transformed_4",
    ]

    assert caplog.record_tuples == [
        (
            "frequenz.client.base.streaming",
            logging.INFO,
            "test_helper: starting to stream",
        ),
        # LogCaptureFixture can't capture tracebacks, so only the error message
        # is checked.
        (
            "frequenz.client.base.streaming",
            logging.ERROR,
            "test_helper: error transforming message: 1",
        ),
        (
            "frequenz.client.base.streaming",
            logging.ERROR,
            "test_helper: error transforming message: 3",
        ),
        (
            "frequenz.client.base.streaming",
            logging.INFO,
            "test_helper: connection closed, stream exhausted",
        ),
        (
            "frequenz.client.base.streaming",
            logging.INFO,
            "test_helper: stopping the stream",
        ),
    ]


@pytest.mark.parametrize("include_events", [True, False])
async def test_retry_next_interval_zero(  # pylint: disable=too-many-arguments
    receiver_ready_event: asyncio.Event,  # pylint: disable=redefined-outer-name
    caplog: pytest.LogCaptureFixture,
    include_events: bool,
) -> None:
    """Test retry logic when next_interval returns 0."""
    caplog.set_level(logging.WARNING)
    error = make_error()
    mock_retry = mock.MagicMock(spec=retry.Strategy)
    mock_retry.next_interval.side_effect = [0, None]
    mock_retry.copy.return_value = mock_retry
    mock_retry.get_progress.return_value = "mock progress"
    helper = streaming.GrpcStreamBroadcaster(
        stream_name="test_helper",
        stream_method=erroring_rpc_mock(error, receiver_ready_event),
        transform=_transformer,
        retry_strategy=mock_retry,
    )

    items: list[str] = []
    events: list[StreamEvent] = []
    async with AsyncExitStack() as stack:
        stack.push_async_callback(helper.stop)

        receiver = helper.new_receiver(include_events=include_events)
        receiver_ready_event.set()
        items, events = await _split_message(receiver)

    assert not items
    assert bool(events) == include_events

    assert mock_retry.next_interval.mock_calls == [mock.call(), mock.call()]
    assert caplog.record_tuples == [
        (
            "frequenz.client.base.streaming",
            logging.WARNING,
            "test_helper: connection ended, retrying mock progress in 0.000 "
            f"seconds. Error: {error}.",
        ),
        (
            "frequenz.client.base.streaming",
            logging.ERROR,
            "test_helper: connection ended, retry limit exceeded (mock progress), "
            f"giving up. Error: {error}.",
        ),
    ]


@pytest.mark.parametrize(
    "include_events", [True, False], ids=["with_events", "without_events"]
)
@pytest.mark.parametrize(
    "error_in_metadata",
    [True, False],
    ids=["with_initial_metadata_error", "iterator_error_only"],
)
async def test_messages_on_retry(
    receiver_ready_event: asyncio.Event,  # pylint: disable=redefined-outer-name
    include_events: bool,
    error_in_metadata: bool,
) -> None:
    """Test that messages are sent on retry."""
    # We need to use a specific instance for all the test here because 2 errors created
    # with the same arguments don't compare equal (grpc.aio.AioRpcError doesn't seem to
    # provide a __eq__ method).
    error = make_error()

    helper = streaming.GrpcStreamBroadcaster(
        stream_name="test_helper",
        stream_method=erroring_rpc_mock(
            error,
            receiver_ready_event,
            num_successes=2,
            should_error_on_initial_metadata_too=error_in_metadata,
        ),
        transform=_transformer,
        retry_strategy=retry.LinearBackoff(limit=1, interval=0.0, jitter=0.0),
        retry_on_exhausted_stream=True,
    )

    items: list[str] = []
    events: list[StreamEvent] = []
    async with AsyncExitStack() as stack:
        stack.push_async_callback(helper.stop)

        receiver = helper.new_receiver(include_events=include_events)
        receiver_ready_event.set()
        items, events = await _split_message(receiver)

    assert items == [
        "transformed_0",
        "transformed_1",
    ]
    if include_events:
        extra_events: list[StreamEvent] = []
        if not error_in_metadata:
            extra_events.append(StreamStarted())
        assert events == [
            StreamStarted(),
            StreamRetrying(timedelta(seconds=0.0), error),
            *extra_events,
            StreamFatalError(error),
        ]
    else:
        assert events == []
