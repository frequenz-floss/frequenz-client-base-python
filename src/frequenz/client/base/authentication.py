# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""An Interceptor that adds the API key to a gRPC call."""

import dataclasses
from typing import Callable

from grpc.aio import (
    ClientCallDetails,
    Metadata,
    UnaryUnaryCall,
    UnaryUnaryClientInterceptor,
)


@dataclasses.dataclass(frozen=True)
class AuthenticationOptions:
    """Options for authenticating to the endpoint."""

    api_key: str
    """The API key to authenticate with."""


# There is an issue in gRPC that causes the type to be unspecifieable correctly here.
class AuthenticationInterceptor(UnaryUnaryClientInterceptor):  # type: ignore[type-arg]
    """An Interceptor that adds HMAC authentication of the metadata fields to a gRPC call."""

    def __init__(self, options: AuthenticationOptions):
        """Create an instance of the interceptor.

        Args:
            options: The options for authenticating to the endpoint.
        """
        self._key = options.api_key

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
        self.add_auth_header(
            client_call_details,
        )
        return await continuation(client_call_details, request)

    def add_auth_header(
        self,
        client_call_details: ClientCallDetails,
    ) -> None:
        """Add the API key as a metadata field to the call.

        The API key is used by the later sign interceptor to calculate the HMAC.
        In addition it is used as a first layer of authentication by the server.

        Args:
            client_call_details: The call details.
        """
        if client_call_details.metadata is None:
            client_call_details.metadata = Metadata()

        client_call_details.metadata["key"] = self._key
