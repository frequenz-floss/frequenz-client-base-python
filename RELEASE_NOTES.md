# Frequenz Client Base Library Release Notes

## Upgrading

The `BaseApiClient` class is generic again. There was too many issues with the new approach, so it was rolled back.

- If you are upgrading from v0.7.x, you should be able to roll back your changes with the upgrade and just keep the new `stub` property.

    ```python
   # Old
   from __future__ import annotations
   import my_service_pb2_grpc
   class MyApiClient(BaseApiClient):
       def __init__(self, server_url: str, *, ...) -> None:
           super().__init__(server_url, ...)
           stub = my_service_pb2_grpc.MyServiceStub(self.channel)
           self._stub: my_service_pb2_grpc.MyServiceAsyncStub = stub  # type: ignore
           ...

       @property
       def stub(self) -> my_service_pb2_grpc.MyServiceAsyncStub:
           if self.channel is None:
               raise ClientNotConnected(server_url=self.server_url, operation="stub")
           return self._stub

   # New
   from __future__ import annotations
   import my_service_pb2_grpc
   from my_service_pb2_grpc import MyServiceStub
   class MyApiClient(BaseApiClient[MyServiceStub]):
       def __init__(self, server_url: str, *, ...) -> None:
           super().__init__(server_url, MyServiceStub, ...)
           ...

       @property
       def stub(self) -> my_service_pb2_grpc.MyServiceAsyncStub:
           """The gRPC stub for the API."""
           if self.channel is None or self._stub is None:
               raise ClientNotConnected(server_url=self.server_url, operation="stub")
           # This type: ignore is needed because we need to cast the sync stub to
           # the async stub, but we can't use cast because the async stub doesn't
           # actually exists to the eyes of the interpreter, it only exists for the
           # type-checker, so it can only be used for type hints.
           return self._stub  # type: ignore
   ```

- If you are upgrading from v0.6.x, you should only need to add the `stub` property to your client class and then use that property instead of `_stub` in your code.

    ```python
       @property
       def stub(self) -> my_service_pb2_grpc.MyServiceAsyncStub:
           """The gRPC stub for the API."""
           if self.channel is None or self._stub is None:
               raise ClientNotConnected(server_url=self.server_url, operation="stub")
           # This type: ignore is needed because we need to cast the sync stub to
           # the async stub, but we can't use cast because the async stub doesn't
           # actually exists to the eyes of the interpreter, it only exists for the
           # type-checker, so it can only be used for type hints.
           return self._stub  # type: ignore
   ```
