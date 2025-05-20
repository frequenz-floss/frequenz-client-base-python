# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the Interceptor functionalities."""

from unittest import mock

from frequenz.client.base.authentication import (
    AuthenticationInterceptor,
    AuthenticationOptions,
)


async def test_auth_interceptor() -> None:
    """Test that the Auth Interceptor adds the correct header."""
    auth: AuthenticationOptions = AuthenticationOptions(api_key="my_key")
    auth_interceptor: AuthenticationInterceptor = AuthenticationInterceptor(
        options=auth
    )

    metadata: dict[str, str] = {}

    client_call_details = mock.MagicMock(method="my_rpc")
    client_call_details.metadata = metadata

    auth_interceptor.add_auth_header(client_call_details)

    assert metadata["key"] == "my_key"
