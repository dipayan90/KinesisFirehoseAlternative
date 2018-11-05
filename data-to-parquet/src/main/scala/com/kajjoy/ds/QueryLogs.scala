package com.kajjoy.ds

case class QueryLogs(
      authorization: String,
      breadcrumb: String,
      environmentName: String,
      host: String,
      documentversion: Long,
      skuid: Long,
      styleid: Long,
      requestUriQuery: String,
      requestUriStem: String,
      timeStamp: String,
      traceContext: String,
      userId: String
                    )
