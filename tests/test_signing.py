# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the Interceptor functionalities."""

from unittest import mock

from frequenz.client.base.signing import _add_hmac


async def test_sign_interceptor() -> None:
    """Test that the HMAC is calculated correctly so that it will match the value of the server."""
    metadata: dict[str, str | bytes] = {"key": "my_key"}

    client_call_details = mock.MagicMock(method="my_rpc")
    client_call_details.metadata = metadata
    client_call_details.method = (
        b"/frequenz.api.wishlist.v1.Wishlist/ElectrifyTheFutureRequest"
    )

    _add_hmac(b"hunter2", client_call_details, 1634567890, b"123456789")

    assert metadata["sig"] == "yNCJYXjac-waeqLhlYJE2cql9rUGIq-7Flz4MAOZefQ".encode(
        "utf-8"
    )
