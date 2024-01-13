require 'active_record'
# BindVisitor was removed in Arel 9 aka Rails 5.2
require 'arel/visitors/bind_visitor' if Arel::VERSION.to_i < 9
require 'odbc'

require 'odbc_adapter/database_limits'
require 'odbc_adapter/database_statements'
require 'odbc_adapter/error'
require 'odbc_adapter/quoting'
require 'odbc_adapter/schema_statements'

require 'odbc_adapter/column'
require 'odbc_adapter/column_metadata'
require 'odbc_adapter/database_metadata'
require 'odbc_adapter/registry'
require 'odbc_adapter/version'

module ActiveRecord
  class Base
    class << self
      # Build a new ODBC connection with the given configuration.
      def odbc_connection(config)
        config = config.symbolize_keys

        connection, config =
          if config.key?(:dsn)
            odbc_dsn_connection(config)
          elsif config.key?(:conn_str)
            odbc_conn_str_connection(config)
          else
            raise ArgumentError, 'No data source name (:dsn) or connection string (:conn_str) specified.'
          end

        database_metadata = ::ODBCAdapter::DatabaseMetadata.new(connection)
        database_metadata.adapter_class.new(connection, logger, config, database_metadata)
      end

      private

      # Connect using a predefined DSN.
      def odbc_dsn_connection(config)
        username   = config[:username] ? config[:username].to_s : nil
        password   = config[:password] ? config[:password].to_s : nil

        # If it includes only the DSN + credentials
        if (config.keys - %i[adapter dsn username password]).empty?
          connection = ODBC.connect(config[:dsn], username, password)
          config = config.merge(username: username, password: password)
        # Support additional overrides, e.g. host: db.example.com
        else
          driver_attrs = config.dup
                               .delete_if { |k, _| %i[adapter username password].include?(k) }
                               .merge(UID: username, PWD: password)

          driver, connection = obdc_driver_connection(driver_attrs)
          config = config.merge(driver: driver)
        end

        [connection, config]
      end

      # Connect using ODBC connection string
      # Supports DSN-based or DSN-less connections
      # e.g. "DSN=virt5;UID=rails;PWD=rails"
      #      "DRIVER={OpenLink Virtuoso};HOST=carlmbp;UID=rails;PWD=rails"
      def odbc_conn_str_connection(config)
        driver_attrs = config[:conn_str].split(';').map { |option| option.split('=', 2) }.to_h
        driver, connection = obdc_driver_connection(driver_attrs)

        [connection, config.merge(driver: driver)]
      end

      def obdc_driver_connection(driver_attrs)
        driver = ODBC::Driver.new
        driver.name = 'odbc'
        driver.attrs = driver_attrs.stringify_keys

        connection = ODBC::Database.new.drvconnect(driver)

        [driver, connection]
      end
    end
  end

  module ConnectionAdapters
    class ODBCAdapter < AbstractAdapter
      include ::ODBCAdapter::DatabaseLimits
      include ::ODBCAdapter::DatabaseStatements
      include ::ODBCAdapter::Quoting
      include ::ODBCAdapter::SchemaStatements

      ADAPTER_NAME = 'ODBC'.freeze
      BOOLEAN_TYPE = 'BOOLEAN'.freeze

      ERR_DUPLICATE_KEY_VALUE     = 23_505
      ERR_QUERY_TIMED_OUT         = 57_014
      ERR_QUERY_TIMED_OUT_MESSAGE = /Query has timed out/

      # The object that stores the information that is fetched from the DBMS
      # when a connection is first established.
      attr_reader :database_metadata

      def initialize(connection, logger, config, database_metadata)
        super(connection, logger, config)
        @database_metadata = database_metadata
      end

      # Returns the human-readable name of the adapter.
      def adapter_name
        ADAPTER_NAME
      end

      # Does this adapter support migrations? Backend specific, as the abstract
      # adapter always returns +false+.
      def supports_migrations?
        true
      end

      # CONNECTION MANAGEMENT ====================================

      # Checks whether the connection to the database is still active. This
      # includes checking whether the database is actually capable of
      # responding, i.e. whether the connection isn't stale.
      def active?
        !!@raw_connection&.connected?
      end

      # Disconnects from the database if already connected, and establishes a
      # new connection with the database.
      def reconnect!
        disconnect!
        connect
        super
      end
      alias reset! reconnect!

      def connect
        @raw_connection =
          if @config[:driver]
            ODBC::Database.new.drvconnect(@config[:driver])
          else
            ODBC.connect(@config[:dsn], @config[:username], @config[:password])
          end
      end

      # Disconnects from the database if already connected. Otherwise, this
      # method does nothing.
      def disconnect!
        super

        @raw_connection.disconnect if active?
        @raw_connection = nil
      end

      # Build a new column object from the given options. Effectively the same
      # as super except that it also passes in the native type.
      # rubocop:disable Metrics/ParameterLists
      def new_column(name, default, sql_type_metadata, null, table_name, default_function = nil, collation = nil, native_type = nil)
        ::ODBCAdapter::Column.new(name, default, sql_type_metadata, null, table_name, default_function, collation, native_type)
      end
      # rubocop:enable Metrics/ParameterLists

      protected

      # Build the type map for ActiveRecord
      def initialize_type_map(map)
        map.register_type 'boolean',              Type::Boolean.new
        map.register_type ODBC::SQL_CHAR,         Type::String.new
        map.register_type ODBC::SQL_LONGVARCHAR,  Type::Text.new
        map.register_type ODBC::SQL_TINYINT,      Type::Integer.new(limit: 4)
        map.register_type ODBC::SQL_SMALLINT,     Type::Integer.new(limit: 8)
        map.register_type ODBC::SQL_INTEGER,      Type::Integer.new(limit: 16)
        map.register_type ODBC::SQL_BIGINT,       Type::BigInteger.new(limit: 32)
        map.register_type ODBC::SQL_REAL,         Type::Float.new(limit: 24)
        map.register_type ODBC::SQL_FLOAT,        Type::Float.new
        map.register_type ODBC::SQL_DOUBLE,       Type::Float.new(limit: 53)
        map.register_type ODBC::SQL_DECIMAL,      Type::Float.new
        map.register_type ODBC::SQL_NUMERIC,      Type::Integer.new
        map.register_type ODBC::SQL_BINARY,       Type::Binary.new
        map.register_type ODBC::SQL_DATE,         Type::Date.new
        map.register_type ODBC::SQL_DATETIME,     Type::DateTime.new
        map.register_type ODBC::SQL_TIME,         Type::Time.new
        map.register_type ODBC::SQL_TIMESTAMP,    Type::DateTime.new
        map.register_type ODBC::SQL_GUID,         Type::String.new

        alias_type map, ODBC::SQL_BIT,            'boolean'
        alias_type map, ODBC::SQL_VARCHAR,        ODBC::SQL_CHAR
        alias_type map, ODBC::SQL_WCHAR,          ODBC::SQL_CHAR
        alias_type map, ODBC::SQL_WVARCHAR,       ODBC::SQL_CHAR
        alias_type map, ODBC::SQL_WLONGVARCHAR,   ODBC::SQL_LONGVARCHAR
        alias_type map, ODBC::SQL_VARBINARY,      ODBC::SQL_BINARY
        alias_type map, ODBC::SQL_LONGVARBINARY,  ODBC::SQL_BINARY
        alias_type map, ODBC::SQL_TYPE_DATE,      ODBC::SQL_DATE
        alias_type map, ODBC::SQL_TYPE_TIME,      ODBC::SQL_TIME
        alias_type map, ODBC::SQL_TYPE_TIMESTAMP, ODBC::SQL_TIMESTAMP
      end

      # Translate an exception from the native DBMS to something usable by
      # ActiveRecord.
      def translate_exception(exception, message:, sql:, binds:)
        error_number = exception.message[/^\d+/].to_i

        if error_number == ERR_DUPLICATE_KEY_VALUE
          ActiveRecord::RecordNotUnique.new(message)
        elsif error_number == ERR_QUERY_TIMED_OUT || exception.message =~ ERR_QUERY_TIMED_OUT_MESSAGE
          ::ODBCAdapter::QueryTimeoutError.new(message)
        else
          super
        end
      end

      # Ensure ODBC is mapping time-based fields to native ruby objects
      def configure_connection
        @raw_connection.use_time = true
      end

      private

      # Can't use the built-in ActiveRecord map#alias_type because it doesn't
      # work with non-string keys, and in our case the keys are (almost) all
      # numeric
      def alias_type(map, new_type, old_type)
        map.register_type(new_type) do |_, *args|
          map.lookup(old_type, *args)
        end
      end
    end
  end
end
