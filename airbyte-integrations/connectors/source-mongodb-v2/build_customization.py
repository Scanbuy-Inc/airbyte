# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagger import Container

# AWS global CA bundle URL for DocumentDB TLS support
# See: https://docs.aws.amazon.com/documentdb/latest/developerguide/ca_cert_rotation.html
AWS_CA_BUNDLE_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
AWS_CA_BUNDLE_PATH = "/airbyte/aws-rds-ca.pem"


async def pre_connector_install(base_image_container: Container) -> Container:
    """
    Downloads the AWS RDS global CA bundle and saves it to a known path inside the image.
    The connector references this file via tlsCAFile in the MongoDB connection string,
    enabling TLS connections to Amazon DocumentDB without requiring users to supply
    certificates manually.

    See: https://github.com/airbytehq/airbyte/issues/10388
    """
    return (
        base_image_container
        .with_exec(["curl", "-fsSL", AWS_CA_BUNDLE_URL, "-o", AWS_CA_BUNDLE_PATH])
    )
