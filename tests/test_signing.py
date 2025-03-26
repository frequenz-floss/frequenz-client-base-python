# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the Interceptor functionalities."""

from unittest import mock

from frequenz.client.base.signing import (
    SigningInterceptor,
    SigningOptions,
)


async def test_sign_interceptor() -> None:
    """Test that the HMAC is calculated correctly so that it will match the value of the server."""
    sign: SigningOptions = SigningOptions(secret="my_secret")
    sign_interceptor: SigningInterceptor = SigningInterceptor(options=sign)

    metadata: dict[str, str | bytes] = {"x-key": "my_key"}

    client_call_details = mock.MagicMock(method="my_rpc")
    client_call_details.metadata = metadata

    sign_interceptor.add_hmac(client_call_details, b"1634567890", b"123456789")

    assert metadata["x-hmac"] == "NJDvrkRZhOPekn5AvPiaJsYTJYCgnLzA-LQFC2D7GNE=".encode(
        "utf-8"
    )
