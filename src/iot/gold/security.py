"""Who may see which data-product rows, enforced by Unity Catalog itself.

Access is data: gold.client_access holds one row per principal (a user or a
service principal) and client they may see. client_id '*' means every client:
that is a platform admin. Adding a client user is an INSERT, not a new group.

Two functions, attached by the pipeline to the data products, which are the
tables people open:

  client_row_filter(client_id)   a row is visible if the principal may see its client
  mask_vin(vin)                  platform admins see the VIN, everyone else its last four characters

Dimensions and facts are building blocks read inside the pipeline. They carry
no filter and are granted only to the platform team: the serving layer is where
security is applied. Unity Catalog applies filters and masks wherever a table is
read, in SQL, dashboards, notebooks or BI tools, so there is no way around them.
"""

from __future__ import annotations

ALL_CLIENTS = "*"
ACCESS_TABLE = "client_access"
ROW_FILTER = "client_row_filter"
VIN_MASK = "mask_vin"

# Shared by the functions and the tests. {principal} is current_user() in Unity
# Catalog, and a literal in tests.
ACCESS_CONDITION = (
    "EXISTS (SELECT 1 FROM {access_table} a "
    "WHERE a.principal = {principal} AND (a.client_id = '*' OR a.client_id = {client}))"
)
ADMIN_CONDITION = (
    "EXISTS (SELECT 1 FROM {access_table} a WHERE a.principal = {principal} AND a.client_id = '*')"
)
MASKED_VIN = "concat(repeat('*', greatest(length({vin}) - 4, 0)), right({vin}, 4))"


def qualified(catalog: str, name: str) -> str:
    return f"{catalog}.gold.{name}"


def row_filter_clause(catalog: str) -> str:
    """The clause a data product takes to be filtered by its client_id column."""
    return f"ROW FILTER {qualified(catalog, ROW_FILTER)} ON (client_id)"


def vin_mask_clause(catalog: str) -> str:
    return f"MASK {qualified(catalog, VIN_MASK)}"


def setup_statements(catalog: str, admin_principal: str) -> list[str]:
    """SQL that creates the access table, both functions, and the first admin. Safe to re-run."""
    if not admin_principal or "'" in admin_principal or "\\" in admin_principal:
        raise ValueError(f"not a valid principal: {admin_principal!r}")
    access = qualified(catalog, ACCESS_TABLE)
    can_see = ACCESS_CONDITION.format(access_table=access, principal="current_user()", client="row_client_id")
    is_admin = ADMIN_CONDITION.format(access_table=access, principal="current_user()")
    return [
        f"""CREATE TABLE IF NOT EXISTS {access} (
              principal STRING NOT NULL COMMENT 'User email or service principal application id',
              client_id STRING NOT NULL COMMENT 'A client id, or * for every client (platform admin)',
              granted_at TIMESTAMP NOT NULL,
              granted_by STRING NOT NULL
            ) COMMENT 'Which principal may see which client''s rows in the data products. * means all.'""",
        f"""CREATE OR REPLACE FUNCTION {qualified(catalog, ROW_FILTER)}(row_client_id STRING)
            RETURNS BOOLEAN
            COMMENT 'Row filter for data products: visible if the principal may see the row''s client.'
            RETURN {can_see}""",
        f"""CREATE OR REPLACE FUNCTION {qualified(catalog, VIN_MASK)}(vin STRING)
            RETURNS STRING
            COMMENT 'Platform admins see the VIN; everyone else sees its last four characters.'
            RETURN CASE WHEN {is_admin} THEN vin ELSE {MASKED_VIN.format(vin="vin")} END""",
        f"""MERGE INTO {access} t
            USING (SELECT '{admin_principal}' AS principal, '*' AS client_id) s
            ON t.principal = s.principal AND t.client_id = s.client_id
            WHEN NOT MATCHED THEN INSERT (principal, client_id, granted_at, granted_by)
            VALUES (s.principal, s.client_id, current_timestamp(), current_user())""",
    ]
