# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the Interceptor functionalities."""

from unittest import mock

from frequenz.client.base.authentication import _add_auth_header


async def test_auth_interceptor() -> None:
    """Test that the Auth Interceptor adds the correct header."""
    metadata: dict[str, str] = {}

    client_call_details = mock.MagicMock(method="my_rpc")
    client_call_details.metadata = metadata

    key = "my_key"

    _add_auth_header(key, client_call_details)

    assert metadata["key"] == "my_key"
