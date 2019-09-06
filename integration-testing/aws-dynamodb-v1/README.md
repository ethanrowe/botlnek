# aws-dynamodb-v1 integration test

Since interactions with dynamodb are essential to the behavior of
this store, and mocking that stuff out is pretty smelly, this integration
test driver provides a docker-compose based way of doing things using
the AWS developer's local dynamodb emulator.

The docker-compose configuration will run a local dynamodb server
in one container, and can run the integration tests in a separate
container.

# Basic usage

From the root of the repository:

```bash
cd integration-testing/aws-dynamodb-v1
docker-compose run test
```

You should see this build the relevant stuff, launch the container for
the local dynamodb emulator, and then run the integration test itself.

The exit code of the `docker-compose run test` call will reflect the
exit code of the underlying `go test` call.

# Integration test particulars

Any test file in the pkg/store/aws/dynamodb/v1 directory tagged with
"integration" will be included in the integration test run.  As of
right now, that is exactly one file ("integration_test.go").

In that file at least, the dynamodb service endpoint is controlled
by the `BOTLNEK_DYNAMODB_ENDPOINT` environment variable, and defaulting
to "http://localhost:8000".  The `docker-compose` setup runs the
local dynamodb emulator and overrides the environment variable to
properly direct dynamodb traffic to the relevant container.

So if you want to run the test against a different backend configuration,
go for it:

```bash
cd pkg/store/aws/dynamodb/v1
BOTLNEK_DYNAMODB_ENDPOINT=http://your-dynamo-backend go test -v -tags=integration
```

