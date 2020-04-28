#' Query available dataset on the Agnitio API
#'
#' @description Queries the Agnitio API regarding available datasets and returns them in an organised list.
#'
#' @return A full listing of datasets with description by Country, Content and Release, to allow for filtering.
#' @export
dataset_list <- function() {
  datasets <- httr::GET("http://agnitio.emsicloud.com/meta", body = NULL, encode = "json", agnitio_settings())
  rawdata <- jsonlite::fromJSON(httr::content(datasets, "text", encoding="UTF-8"), simplifyVector = FALSE)
  datasets <- rawdata[[1]] %>% purrr::transpose()
  name <- unlist(datasets[[1]])
  versions <- as.numeric(purrr::map_chr(datasets[[2]], length))
  datasets <- data.frame(dataset = c(rep(name, versions)), version = c(unlist(datasets[[2]])))
  final <- datasets %>% dplyr::tbl_df() %>%
    dplyr::mutate(Dataset = as.character(dataset), Version = as.character(version), Country = substr(dataset,
                                                                                                     6, 7), Content = substr(dataset, 9, nchar(Dataset)), Identifier = paste(dataset, "/", version, sep = "")) %>%
    dplyr::arrange(Country) %>%
    dplyr::select(-dataset, -version, -Dataset, Country, Content, Version, Identifier) %>%
    dplyr::group_by(Country, Content)
  return(final)
}

#' Query the details of an Agnitio API dataset
#'
#' @description Identify the available metrics and required dimensions for a dataset held on Emsi Episteme.
#'
#' @param country The two-digit country identifier for the dataset.
#' @param content The keyword identifier for the content of the dataset (e.g. \code{"Occupation"}, \code{"Industry"}).
#' @param release The release or version identifier for the dataset (e.g. \code{"2016.1"}).
#' @return A list with two elements: \code{Metrics} lists the Metrics available within the dataset and \code{Dimensions}
#' lists the dimensions requiring constraint when making a request for data. Note that \code{Metrics}
#'  does not include Location Quotients and Shift-Share, which are derived from the metrics here.
#' @examples
#' dataset_detail("UK","Occupation","2016.1")
#' @export
dataset_detail <- function(country, content, release) {
  dataset <- paste("emsi", tolower(country), tolower(content), sep = ".")
  URI <- paste("http://agnitio.emsicloud.com/meta/dataset", dataset, release, sep = "/")
  parameters <- httr::GET(URI, body = NULL, encode = "json", agnitio_settings())
  parameters <- jsonlite::fromJSON(httr::content(parameters, "text", encoding="UTF-8"), simplifyDataFrame = TRUE)
  parameters[c("dimensions","metrics")]
}

#' Query the details of an Agnitio API dataset
#'
#' @description Identify the available metrics and required dimensions for a dataset held on Emsi Episteme.
#'
#' @param country The two-digit country identifier for the dataset.
#' @param content The keyword identifier for the content of the dataset (e.g. \code{"Occupation"}, \code{"Industry"}).
#' @param release The release or version identifier for the dataset (e.g. \code{"2016.1"}).
#' @param dimension The dimension you wish to explore.
#' @return A data frame with the hierarchy and categories.
#' @examples
#' dataset_detail("UK","Occupation","2016.1")
#' @export
dataset_dimension <- function(country, content, release, dimension) {
  dataset <- paste("emsi", tolower(country), tolower(content), sep = ".")
  URI <- paste("http://agnitio.emsicloud.com/meta/dataset", dataset, release, dimension, sep = "/")
  parameters <- httr::GET(URI, body = NULL, encode = "json", agnitio_settings())
  parameters <- jsonlite::fromJSON(httr::content(parameters, "text", encoding="UTF-8"), simplifyDataFrame = TRUE)
  parameters$hierarchy %>% dplyr::tbl_df()
}
