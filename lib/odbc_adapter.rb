require "active_record/connection_adapters/odbc_adapter"
require "odbc_adapter/adapters/null_odbc_adapter"
require "odbc_adapter/adapters/mysql_odbc_adapter"
require "odbc_adapter/adapters/postgresql_odbc_adapter"
require "odbc_adapter/adapters/snowflake_odbc_adapter"

# The pre-7.2 approach to mapping adapter strings like "odbc" to an adapter
# class meant that "odbc" would map to `ODBCAdapter`, which would then use
# `DatabaseMetadata` to pick the correct adapter via `dbms_name`. The 7.2 way
# involves a registered mapping, and thus `adapter: odbc` is not a dynamic
# lookup. We do not need anything but Snowflake support, so preserve existing
# `adapter: "odbc"` references as mapped to Snowflake.
ActiveRecord::ConnectionAdapters.register "odbc", "ODBCAdapter::Adapters::SnowflakeODBCAdapter", "odbc_adapter/adapters/snowflake_odbc_adapter"
ActiveRecord::ConnectionAdapters.register "snowflake_odbc", "ODBCAdapter::Adapters::SnowflakeODBCAdapter", "odbc_adapter/adapters/snowflake_odbc_adapter"
