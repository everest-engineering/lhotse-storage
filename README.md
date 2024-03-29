# File storage support for Lhotse

[![Build status](https://badge.buildkite.com/d5a0c236b0dd90d368dd448f4010eaee15d409f5c6c8a20b2a.svg?branch=main)](https://buildkite.com/everest-engineering/lhotse-storage) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=everest-engineering_lhotse-storage&metric=alert_status)](https://sonarcloud.io/dashboard?id=everest-engineering_lhotse-storage)

This is a **standalone library** as well as a supporting repository
for [Lhotse](https://github.com/everest-engineering/lhotse), a starter kit for writing event sourced web
applications following domain driven design principles.

# Purpose

This library implements two file stores: one is referred to as _permanent_, the other as the _ephemeral_ store. The
permanent file store is for storing critical files that, such as user uploads, cannot be recovered. The ephemeral store
is for non-critical files that can be regenerated by the system either dynamically or via an event replay.

Our file store implementation automatically deduplicates files. Storing a file whose contents matches a previous file
will return a (new) file identifier mapping to the original. The most recently stored file will then be silently
removed.

File stores need backing service such as a blob store or filesystem. This library supports an in-memory file store for
testing and development, [Mongo GridFS](https://docs.mongodb.com/manual/core/gridfs/) and AWS S3.

### Configuring the In-Memory Filestore

The in-memory filestore backend is intended only for development and testing. This filestore is not distributed so will
not work well when running multiple instances of the application in HA mode. A locally hosted or AWS hosted S3
compatible filestore is a better bet in this instance.

```
application.filestore.backend=inMemory
```

#### Configuring Mongo GridFS

Set the application property:

```
application.filestore.backend=mongoGridFs
```

#### Configuring AWS S3

Set the following application properties:

```
application.filestore.backend=awsS3
application.filestore.awsS3.buckets.permanent=sample-bucket-permanent
application.filestore.awsS3.buckets.ephemeral=sample-bucket-ephemeral
```

We rely
on [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html)
and [DefaultAwsRegionProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/regions/DefaultAwsRegionProviderChain.html)
for fetching AWS credentials and the AWS region.

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[![License: EverestEngineering](https://img.shields.io/badge/Copyright%20%C2%A9-EVERESTENGINEERING-blue)](https://everest.engineering)

> Talk to us `hi@everest.engineering`.
