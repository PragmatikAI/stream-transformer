# Pragmatik Transformer

A service that transforms Snowplow enriched events into a flattened format suitable for downstream processing.

## Overview

The Pragmatik Transformer takes enriched Snowplow events and:

1. Flattens nested JSON structures into a single level
2. Renames fields to be more readable and consistent
3. Splits certain event types (like list_view) into multiple events
4. Handles various Snowplow contexts and unstructured events

## Usage

The service provides utility functions in `transformer/utils.py`:

- `flatten_event()` - Flattens a single event's nested structure
- `split_event()` - Splits list_view events into multiple events (one per product)
- `convert_context()` - Converts Snowplow context fields
- `convert_unstruct_event()` - Converts unstructured event fields

## Run tests
cd to root of dir
```bash
python -m test.utils
```

## Build
just push it to github and it'll get pushed to dockerhub

don't for get to tag the commit for git action to run