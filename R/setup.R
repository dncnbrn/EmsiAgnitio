#' Set login details for the Agnitio API.
#'
#' @description  Sets login details for Emsi Agnitio as environment variables which will be used to authenticate all queries to the API.
#'
#' @param username is the episteme-username variable for Episteme.
#' @param password is the episteme-secret1 variable for Episteme.
#' @return The variables are placed within the environment for use by other functions.
#' @importFrom magrittr "%>%"
#' @export
agnitio_login <- function(username, password) {
  Sys.setenv(EMSIUN = username, EMSISECRET = password)
}

#' Background function which is used to take Emsi Agnitio login details set by \code{\link{EpistemeLogin}} and obtains a token.
#' @export
agnitio_token <- function() {
  raw_token <- httr::POST("https://auth.emsicloud.com/connect/token",
                          body=list('client_id'=Sys.getenv("EMSIUN"),
                                    'client_secret'=Sys.getenv("EMSISECRET"),
                                    'grant_type'="client_credentials",
                                    'scope'="emsiauth"),
                          httr::add_headers(`Content-Type`="application/x-www-form-urlencoded"),
                          encode="form"#,
                          # httr::verbose(data_out=TRUE, data_in = TRUE, info = TRUE, ssl=TRUE)
  )
  proc_token <- jsonlite::fromJSON(httr::content(raw_token, "text",encoding="UTF-8"),
                                   simplifyVector = TRUE)
  Sys.setenv(EMSITOKEN=proc_token$access_token)
  Sys.setenv(EMSIEXPIRY=Sys.time()+proc_token$expires_in-120)
}

#' Background function which is used to check if a token is available and valid, and obtain one if not.
#' @importFrom magrittr "%>%"
#' @export
agnitio_settings <- function() {
  if(Sys.getenv("EMSITOKEN")=="") {
    agnitio_token()
  }
  if(as.numeric(Sys.getenv("EMSIEXPIRY"))<Sys.time()) {
    agnitio_token()
  }
  return(httr::add_headers(`Authorization`=paste("bearer",Sys.getenv("EMSITOKEN"),sep=" ")))
}
