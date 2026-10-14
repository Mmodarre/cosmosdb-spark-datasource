# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: on the repository's **Security** tab, choose
**Report a vulnerability**, or open
[a new advisory](https://github.com/Mmodarre/cosmosdb-spark-datasource/security/advisories/new).
Do not open a public issue, pull request or discussion for a vulnerability.

Include the affected version, a description of the issue and its impact, and the steps or code
to reproduce it. Do not include real account endpoints, keys or tokens.

The fix is developed in a private fork, and the advisory is published together with the release
that fixes the issue.

## Supported versions

Only the latest release receives security fixes.

## Scope

Examples of issues in scope: secrets or tokens written to logs, exception messages, `repr()`
output or Spark plans; credentials shared between principals through the client cache; options
that bypass TLS verification.

Vulnerabilities in Azure Cosmos DB, the `azure-cosmos` or `azure-identity` SDKs, Apache Spark or
Databricks should be reported to their maintainers.
