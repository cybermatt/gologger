===================
Just another logger
===================

Stack
-----

    * GO
    * RabbitMQ
    * PostgreSQL


Table structure
---------------
::

    CREATE TABLE gologger.log (
        id bigserial NOT NULL,
        stamp_begin timestamptz NULL,
        stamp_end timestamptz NULL,
        func_name text NULL,
        response jsonb NULL,
        service_name text NULL,
        external_key text NULL,
        request jsonb NULL,
        response_headers jsonb NULL,
        request_headers jsonb NULL,
        url text NULL,
        http_code int4 NULL
    );
