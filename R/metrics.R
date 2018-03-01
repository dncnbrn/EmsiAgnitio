#' @importFrom magrittr "%>%"
#' @export
metricalc <- function(metrics, name, base, geoparent, along) {
  if (metrics == "LQ") {
    metout <- list(name = "LocationQuotient", geoparent = geoparent, along = along)
  }
  if (metrics == "SS") {
    metout <- list(name = "ShiftShare", geoparent = geoparent, along = along, base = as.character(base))
  }
  if (metrics == "OP") {
    metout <- list(name = "LegacyOpenings", startYear = substr(as.character(base),6,9), endYear = substr(as.character(name),6,9))
  }
  return(metout)
}

#' Specify metrics for an Agnitio API query
#'
#' @description Takes a data frame of required metrics and necessary supporting criteria and specifies them ready for an Emsi Agnitio data pull.
#'
#' @param metricdf at minimum, a data frame with two columns: \code{name} sets out the names for the metrics on Agnitio and
#' \code{as} sets out the labels given to those metrics. Where using derivative metrics (Location Quotients and
#'  Shift-Share), additional columns are required in the form of \code{metrics} to specify if they are \emph{"LQ"} or
#'  \emph{"SS"} and, for Shift-Share, a \code{base} column identifies the comparison metric for the year. A data frame may pass with
#'  only the \code{name} specified -- metrics will be given the Agnitio name as a label.
#' @param geoparent is required for derivative metrics, and is a geographical code identifing the parent geographical unit for analysis.
#' @param along is required for derivative metrics, and reflects the intended domain for analysis (e.g. "Industry" or "Occupation").
#' @return A prepared data frame which will be ready for inclusion in a data pull query.
#' @examples
#' met1 <- data.frame(names=c("Jobs.2016","Jobs.2022"), as=c("Jobs.2016","Jobs.2022"))
#' metricmaker(met1)
#' met2 <- data.frame(name=c("Jobs.2016","Jobs.2016","Jobs.2016"),as=c("Jobs16","LQ16","SS16"),metrics=c(NA,"LQ","SS"),base=c(NA,NA,"Jobs.2003"))
#' metricmaker(met2, "GB", "Occupation")
#' @importFrom magrittr "%>%"
#' @export
metricmaker <- function(metricdf, geoparent, along) {
  if (("as" %in% colnames(metricdf))==FALSE) {
    metricdf$as <- metricdf$name
  }
  if (ncol(metricdf) == 2) {
    metricdf$metrics <- c(rep(NA, nrow(metricdf)))
  }
  if (ncol(metricdf) == 3 & nrow(metricdf[!is.na(metricdf$metrics), ]) == 0) {
    metrics <- metricdf
    metrics$metrics <- NULL
  }
  if (ncol(metricdf) >= 3 & nrow(metricdf[is.na(metricdf$metrics), ]) == 0) {
    metrics <- metricdf %>%
      dplyr::mutate(geoparent = geoparent, along = along) %>%
      dplyr::group_by(name, as) %>%
      dplyr::do(operation = metricalc(.$metrics, .$name,
                                      .$base, .$geoparent, .$along))
  }
  if (ncol(metricdf) >= 3 & nrow(metricdf[!is.na(metricdf$metrics), ]) > 0 & nrow(metricdf[is.na(metricdf$metrics), ]) > 0) {
    a <- metricdf %>% dplyr::filter(is.na(metrics)) %>% dplyr::select(name, as)
    a$operation <- list(NA)
    b <- metricdf %>% dplyr::filter(!is.na(metrics))
    b <- b %>%
      dplyr::mutate(geoparent = geoparent, along = along) %>%
      dplyr::group_by(name, as) %>%
      dplyr::do(operation = metricalc(.$metrics, .$name,
                                      .$base, .$geoparent, .$along))
    metrics <- dplyr::bind_rows(a, b)
    rm(a, b)
  }
  return(metrics)
}
