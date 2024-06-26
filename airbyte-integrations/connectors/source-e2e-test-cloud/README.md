# End-to-End Testing Source Cloud Variant

This is the Cloud variant of the [E2E Test Source](https://docs.airbyte.io/integrations/sources/e2e-test). It only allows the "continuous feed" mode a finite number of record messages. The two legacy modes ("infinite feed" and "exception after n") are excluded from cloud because 1) the catalog is not customized under those modes, and 2) the connector should not emit infinite records, which may result in high cost accidentally.

## Local development

#### Building via Gradle

From the Airbyte repository root, run:

```
./gradlew :airbyte-integrations:connectors:source-e2e-test-cloud:build
```

#### Create credentials

No credential is needed for this connector.

### Locally running the connector docker image

#### Build

Build the connector image via Gradle:

```
./gradlew :airbyte-integrations:connectors:source-e2e-test-cloud:buildConnectorImage
```

Once built, the docker image name and tag on your host will be `airbyte/source-e2e-test-cloud:dev`.
the Dockerfile.

#### Run

Then run any of the connector commands as follows:

```
docker run --rm airbyte/source-e2e-test-cloud:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-e2e-test-cloud:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-e2e-test-cloud:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-e2e-test-cloud:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

#### Cloud variant

The cloud version of this connector only allows the `CONTINUOUS FEED` mode. When this mode is changed, please make sure that the cloud variant is updated and published accordingly as well.

## Testing

We use `JUnit` for Java tests.

### Unit and Integration Tests

Place unit tests under `src/test/io/airbyte/integrations/sources/e2e-test`.

#### Acceptance Tests

Airbyte has a standard test suite that all destination connectors must pass. See example(s) in
`src/test-integration/java/io/airbyte/integrations/sources/e2e-test/`.

### Using gradle to run tests

All commands should be run from airbyte project root.
To run unit tests:

```
./gradlew :airbyte-integrations:connectors:sources-e2e-test:unitTest
```

To run acceptance and custom integration tests:

```
./gradlew :airbyte-integrations:connectors:sources-e2e-test:integrationTest
```

## Dependency Management

### Publishing a new version of the connector

You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?

1. Make sure your changes are passing our test suite: `airbyte-ci connectors --name=source-e2e-test-cloud test`
2. Bump the connector version in `metadata.yaml`: increment the `dockerImageTag` value. Please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors).
3. Make sure the `metadata.yaml` content is up to date.
4. Make the connector documentation and its changelog is up to date (`docs/integrations/sources/e2e-test.md`).
5. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
6. Pat yourself on the back for being an awesome contributor.
7. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
