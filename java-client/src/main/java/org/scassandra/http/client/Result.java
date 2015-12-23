package org.scassandra.http.client;

public enum Result {
    success,
    read_request_timeout,
    unavailable,
    write_request_timeout,
    server_error,
    protocol_error,
    bad_credentials,
    overloaded,
    is_bootstrapping,
    truncate_error,
    syntax_error,
    unauthorized,
    invalid,
    config_error,
    already_exists,
    unprepared,
    closed_connection
}
