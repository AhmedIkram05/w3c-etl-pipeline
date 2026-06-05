{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH raw_status AS (
    SELECT DISTINCT
        COALESCE(status, -1) AS status_code,
        COALESCE(sub_status, -1) AS sub_status,
        COALESCE(win32_status, -1) AS win32_status
    FROM {{ source('w3c', 'raw_enriched') }}
),

status_entries AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY rs.status_code, rs.sub_status, rs.win32_status) AS status_sk,
        rs.status_code,
        rs.sub_status,
        rs.win32_status,
        CASE
            WHEN rs.status_code >= 500 THEN 'Server Error'
            WHEN rs.status_code >= 400 THEN 'Client Error'
            WHEN rs.status_code >= 300 THEN 'Redirect'
            WHEN rs.status_code >= 200 THEN 'Success'
            WHEN rs.status_code >= 100 THEN 'Informational'
            ELSE 'Unknown'
        END AS status_category,
        CASE
            WHEN rs.status_code = 200 THEN 'OK'
            WHEN rs.status_code = 201 THEN 'Created'
            WHEN rs.status_code = 204 THEN 'No Content'
            WHEN rs.status_code = 301 THEN 'Moved Permanently'
            WHEN rs.status_code = 302 THEN 'Found (Redirect)'
            WHEN rs.status_code = 304 THEN 'Not Modified'
            WHEN rs.status_code = 400 THEN 'Bad Request'
            WHEN rs.status_code = 401 THEN 'Unauthorized'
            WHEN rs.status_code = 403 THEN 'Forbidden'
            WHEN rs.status_code = 404 THEN 'Not Found'
            WHEN rs.status_code = 405 THEN 'Method Not Allowed'
            WHEN rs.status_code = 408 THEN 'Request Timeout'
            WHEN rs.status_code = 500 THEN 'Internal Server Error'
            WHEN rs.status_code = 502 THEN 'Bad Gateway'
            WHEN rs.status_code = 503 THEN 'Service Unavailable'
            WHEN rs.status_code = 504 THEN 'Gateway Timeout'
            ELSE 'Other'
        END AS status_label,
        CASE
            WHEN rs.status_code = 200 THEN 'OK - The request succeeded'
            WHEN rs.status_code = 201 THEN 'Created - Resource created successfully'
            WHEN rs.status_code = 204 THEN 'No Content - Request succeeded with no response body'
            WHEN rs.status_code = 301 THEN 'Moved Permanently - Resource has moved to a new URL'
            WHEN rs.status_code = 302 THEN 'Found (Redirect) - Resource temporarily moved'
            WHEN rs.status_code = 304 THEN 'Not Modified - Cached version is still valid'
            WHEN rs.status_code = 400 THEN 'Bad Request - Server could not understand the request'
            WHEN rs.status_code = 401 THEN 'Unauthorized - Authentication is required'
            WHEN rs.status_code = 403 THEN 'Forbidden - Access to the resource is denied'
            WHEN rs.status_code = 404 THEN 'Not Found - Server could not find the requested resource'
            WHEN rs.status_code = 405 THEN 'Method Not Allowed - HTTP method not supported for this resource'
            WHEN rs.status_code = 408 THEN 'Request Timeout - Client did not send a request within the timeout period'
            WHEN rs.status_code = 500 THEN 'Internal Server Error - Server encountered an unexpected condition'
            WHEN rs.status_code = 502 THEN 'Bad Gateway - Upstream server returned an invalid response'
            WHEN rs.status_code = 503 THEN 'Service Unavailable - Server is temporarily unable to handle the request'
            WHEN rs.status_code = 504 THEN 'Gateway Timeout - Upstream server failed to respond in time'
            ELSE CONCAT('Other - ', rs.status_code, ' status code')
        END AS description,
        CASE
            WHEN rs.status_code >= 500 THEN 'Critical'
            WHEN rs.status_code >= 400 THEN 'Error'
            WHEN rs.status_code >= 300 THEN 'Warning'
            WHEN rs.status_code >= 200 THEN 'Info'
            WHEN rs.status_code >= 100 THEN 'Info'
            ELSE 'Unknown'
        END AS severity
    FROM raw_status rs
)

SELECT * FROM status_entries
