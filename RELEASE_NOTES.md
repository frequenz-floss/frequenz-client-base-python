# Frequenz Client Base Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* The streaming client now also sends state change events out. Usage example:

    ```python
    recv = streamer.new_receiver()

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
* In the `streaming` module, the new function `filter_stream_events` can be used to filter out stream events and retain the old behavior.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
