#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_event_bridge import DestinationEventBridge

if __name__ == "__main__":
    DestinationEventBridge().run(sys.argv[1:])
