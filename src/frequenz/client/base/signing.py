# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""An Interceptor that adds HMAC signature of the metadata fields to a gRPC call."""

import dataclasses
import hmac
import logging
import secrets
import time
from base64 import urlsafe_b64encode
from typing import Any, Callable

from grpc.aio import (
    ClientCallDetails,
    UnaryUnaryCall,
    UnaryUnaryClientInterceptor,
)

_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class SigningOptions:
    """Options for message signing of messages."""

    secret: str
    """The secret to sign the message with."""


# There is an issue in gRPC that causes the type to be unspecifieable correctly here.
class SigningInterceptor(UnaryUnaryClientInterceptor):  # type: ignore[type-arg]
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
        self.add_hmac(
            client_call_details,
            int(time.time()).to_bytes(8, "big"),
            secrets.token_bytes(16),
        )
        return await continuation(client_call_details, request)

    def add_hmac(
        self, client_call_details: ClientCallDetails, ts: bytes, nonce: bytes
    ) -> None:
        """Add the HMAC authentication to the metadata fields of the call details.

        The extra headers are directly added to the client_call details.

        Args:
            client_call_details: The call details.
            ts: The timestamp to use for the HMAC.
            nonce: The nonce to use for the HMAC.
        """
        if client_call_details.metadata is None:
            _logger.error(
                "No metadata found, cannot extract an api key. Therefore, cannot sign the request."
            )
            return

        key: Any = client_call_details.metadata.get("x-key")
        if key is None:
            _logger.error("No key found in metadata, cannot sign the request.")
            return
        hmac_obj = hmac.new(self._secret, digestmod="sha256")
        hmac_obj.update(key.encode())
        hmac_obj.update(ts)
        hmac_obj.update(nonce)

        hmac_obj.update(client_call_details.method.encode())

        client_call_details.metadata["x-ts"] = ts
        client_call_details.metadata["x-nonce"] = nonce
        client_call_details.metadata["x-hmac"] = urlsafe_b64encode(hmac_obj.digest())
