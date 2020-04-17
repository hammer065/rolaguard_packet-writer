# RoLaGuard Community Edition

## Packet Writer

This repository contains the source code of RoLaGuard packet-writer. This module receives messages in JSON format from Data Collectors queues in RabbitMQ and saves them to DB.

To access the main project with instructions to easily run the rolaguard locally visit the [RoLaGuard](./../README.md) repository. For contributions, please visit the [CONTRIBUTIONS](./../CONTRIBUTING.MD) file.
​

## Saving raw messages

Besides sending the parsed packets to the database, the raw messages can be saved for a posterior analysis. This can be done both in S3 (simply set the needed enviroments variables) or inside the docker image. By default, the raw messages are saved inside the image, in the path:

{year}/{month}/{day}/{collector}/messages_collector_{data_collector_id}_{full_date}.json.gz

## Build the docker image

Build a docker image locally:

```bash
docker build -t rolaguard-packet-writer .
```
