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

      class << self
        # Build a new ODBC connection with the given configuration.
        def new_client(config)
          config = config.symbolize_keys

          connection, config =
            if config.key?(:dsn)
              odbc_dsn_connection(config)
            elsif config.key?(:conn_str)
              odbc_conn_str_connection(config)
            else
              raise ArgumentError, 'No data source name (:dsn) or connection string (:conn_str) specified.'
            end

          [connection, config]
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

        # Can't use the built-in ActiveRecord map#alias_type because it doesn't
        # work with non-string keys, and in our case the keys are (almost) all
        # numeric
        def alias_type(map, new_type, old_type)
          map.register_type(new_type) do |_, *args|
            map.lookup(old_type, *args)
          end
        end
      end

      # The object that stores the information that is fetched from the DBMS
      # when a connection is first established.
      attr_reader :database_metadata

      TYPE_MAP = Type::TypeMap.new.tap { |m| initialize_type_map(m) }

      def initialize(...)
        super

        @raw_connection, @config = self.class.new_client(@config)
        # Standard adapters expect lazily connected adapters on newer versions
        # of Rails (7.1+), whereas we eagerly connect. To ensure the connection
        # is properly configured from the start, we call `configure_connection`
        # explicitly.
        configure_connection
        @database_metadata = ::ODBCAdapter::DatabaseMetadata.new(@raw_connection)

        @database_metadata
      end

      def active?
        @raw_connection.connected?
      end

      def reconnect
        disconnect!
        @raw_connection, @config = self.class.new_client(@config)
        # This is probably unnecessary and should happen automatically in
        # `AbstractAdapter`, but just in case...
        configure_connection
      end

      def disconnect!
        @raw_connection.disconnect if @raw_connection.connected?
      end

      protected

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

      def initialize_type_map(m)
        self.class.initialize_type_map(m)
      end
    end
  end
end
