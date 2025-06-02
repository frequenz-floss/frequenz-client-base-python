# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Test filtering of stream events."""

import logging
from datetime import timedelta
from typing import Tuple, Type

import pytest
from frequenz.channels import Broadcast

from frequenz.client.base.streaming import (
    StreamEvent,
    StreamFatalError,
    StreamRetrying,
    StreamStarted,
    filter_stream_events,
)


@pytest.mark.parametrize(
    "filter_events",
    (
        (StreamStarted, StreamRetrying, StreamFatalError),
        (StreamRetrying, StreamFatalError),
        (StreamFatalError,),
        (),
        (StreamStarted, StreamRetrying),
    ),
)
async def test_filter_stream_events(
    filter_events: Tuple[Type[StreamEvent], ...],
) -> None:
    """Test filtering all events."""
    channel = Broadcast[int | StreamEvent](name="FilterStreamEventsTestChannel")

    receiver = filter_stream_events(channel.new_receiver(), filter_events)
    sender = channel.new_sender()

    events = (
        StreamStarted(),
        1,
        2,
        3,
        StreamRetrying(delay=timedelta(seconds=1)),
        4,
        5,
        6,
        StreamFatalError(exception=Exception("Test error")),
    )

    num_samples = 6
    num_received_samples = 0

    for event in events:
        logging.info("Sending event: %s", event)
        await sender.send(event)

    await channel.close()

    async for event in receiver:
        logging.info("Received event: %s", event)
        if isinstance(event, int):
            num_received_samples += 1
        else:
            assert not isinstance(
                event, filter_events
            ), "Received unexpected event type"

    assert num_received_samples == num_samples, "Unexpected number of samples received"
