# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""An Interceptor that adds HMAC signature of the metadata fields to a gRPC call."""

import dataclasses
import hmac
import logging
import secrets
import time
from base64 import urlsafe_b64encode
from typing import Any, AsyncIterable, Callable

from grpc.aio import (
    ClientCallDetails,
    UnaryStreamCall,
    UnaryStreamClientInterceptor,
    UnaryUnaryCall,
    UnaryUnaryClientInterceptor,
)

_logger = logging.getLogger(__name__)


def _add_hmac(
    secret: bytes, client_call_details: ClientCallDetails, ts: int, nonce: bytes
) -> None:
    """Add the HMAC authentication to the metadata fields of the call details.

    The extra headers are directly added to the client_call details.

    Args:
        secret: The symmetric secret shared with the service.
        client_call_details: The call details.
        ts: The timestamp to use for the HMAC.
        nonce: The nonce to use for the HMAC.
    """
    if client_call_details.metadata is None:
        _logger.error(
            "No metadata found, cannot extract an api key. Therefore, cannot sign the request."
        )
        return

    key: Any = client_call_details.metadata.get("key")
    if key is None:
        _logger.error("No key found in metadata, cannot sign the request.")
        return

    # Make into a base10 integer string and then encode to bytes
    # We can not use a raw bytes timestamp as the underlying network library
    # really hates zero bytes in the metadata values
    ts_bytes = str(ts).encode()
    nonce_bytes = urlsafe_b64encode(nonce)

    hmac_obj = hmac.new(secret, digestmod="sha256")
    hmac_obj.update(key.encode())
    hmac_obj.update(ts_bytes)
    hmac_obj.update(nonce_bytes)

    # Once again, gRPC is mistyped
    hmac_obj.update(client_call_details.method.split(b"/")[-1])  # type: ignore[arg-type]

    client_call_details.metadata["ts"] = ts_bytes
    client_call_details.metadata["nonce"] = nonce_bytes
    # By definition the signature is base64 encoded _without_ the padding, so we strip that
    client_call_details.metadata["sig"] = urlsafe_b64encode(hmac_obj.digest()).strip(
        b"="
    )


@dataclasses.dataclass(frozen=True)
class SigningOptions:
    """Options for message signing of messages."""

    secret: str
    """The secret to sign the message with."""


# There is an issue in gRPC which means the type can not be specified correctly here.
class SigningInterceptorUnaryUnary(UnaryUnaryClientInterceptor):  # type: ignore[type-arg]
    """An Interceptor that adds HMAC authentication of the metadata fields to a gRPC call."""

    def __init__(self, options: SigningOptions):
        """Create an instance of the interceptor.

        Args:
            options: The options for signing the message.
        """
        self._secret = options.secret.encode()

    async def intercept_unary_unary(
        self,
        continuation: Callable[
            [ClientCallDetails, object], UnaryUnaryCall[object, object]
        ],
        client_call_details: ClientCallDetails,
        request: object,
    ) -> object:
        """Intercept the call to add HMAC authentication to the metadata fields.

        This is a known method from the base class that is overridden.

        Args:
            continuation: The next interceptor in the chain.
            client_call_details: The call details.
            request: The request object.

        Returns:
            The response object (this implementation does not modify the response).
        """
        _add_hmac(
            self._secret,
            client_call_details,
            int(time.time()),
            secrets.token_bytes(16),
        )
        return await continuation(client_call_details, request)


# There is an issue in gRPC which means the type can not be specified correctly here.
class SigningInterceptorUnaryStream(UnaryStreamClientInterceptor):  # type: ignore[type-arg]
    """An Interceptor that adds HMAC authentication of the metadata fields to a gRPC call."""

    def __init__(self, options: SigningOptions):
        """Create an instance of the interceptor.

        Args:
            options: The options for signing the message.
        """
        self._secret = options.secret.encode()

    async def intercept_unary_stream(
        self,
        continuation: Callable[
            [ClientCallDetails, Any], UnaryStreamCall[object, object]
        ],
        client_call_details: ClientCallDetails,
        request: Any,
    ) -> AsyncIterable[object] | UnaryStreamCall[object, object]:
        """Intercept the call to add HMAC authentication to the metadata fields.

        This is a known method from the base class that is overridden.

        Args:
            continuation: The next interceptor in the chain.
            client_call_details: The call details.
            request: The request object.

        Returns:
            The response object (this implementation does not modify the response).
        """
        _add_hmac(
            self._secret,
            client_call_details,
            int(time.time()),
            secrets.token_bytes(16),
        )
        return await continuation(client_call_details, request)  # type: ignore
