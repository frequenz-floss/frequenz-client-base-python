# Frequenz Client Base Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* The streaming client, when using `new_receiver(include_events=True)`, will now return a receiver that yields stream notification events, such as `StreamStarted`, `StreamRetrying`, and `StreamFatalError`. This allows you to monitor the state of the stream:

    ```python
    recv = streamer.new_receiver(include_events=True)

    for msg in recv:
        match msg:
            case StreamStarted():
                print("Stream started")
            case StreamRetrying(delay, error):
                print(f"Stream stopped and will retry in {delay}: {error or 'closed'}")
            case StreamFatalError(error):
                print(f"Stream will stop because of a fatal error: {error}")
            case int() as output:
                print(f"Received message: {output}")
    ```

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
