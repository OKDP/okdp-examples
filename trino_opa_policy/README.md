# Trino authorization policies using OPAL

This repository contains an example Rego policy and JSON data file for [OPA (Open Policy Agent)](https://www.openpolicyagent.org/), tailored for [Trino](https://trino.io/) authorization with OPAL.

The policy uses only the username returned by Trino to OPA at `input.context.identity.user`. It does not authorize from OIDC/JWT roles, claims, or groups.

Although the Trino OPA request model includes `input.context.identity.groups`, Trino does not populate that field directly from OIDC `groups` token claims. Trino groups are resolved through Trino group providers such as `file` or `ldap`, as documented in [Trino group mapping](https://trino.io/docs/current/security/group-mapping.html). In this example, permissions are therefore assigned directly to Trino users in `data.json`.

For interactive OAuth2 logins, the configured Trino principal is `preferred_username`, so users are expected to appear as names such as `bob`, `alice`, or `adm`. For JWT/service-account access, the configured principal is `client_id`, so service accounts are expected to appear as names such as `svc-trino-polaris-writer`.

The lists of users, operations, and permissions are non exhaustive.

The goal of this repository is to provide a test environment for policies and data to be pulled by an [OPAL server](https://docs.opal.ac/) to an OPA server embedded in an OKDP cluster.
