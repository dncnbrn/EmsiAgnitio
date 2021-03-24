#' @export
pullquery <- function(country, content, release, constraints, metrics) {
  emsi_URL <- paste("http://agnitio.emsicloud.com/emsi.", country, ".", content, "/", release, sep = "")
  body <- list(metrics = metrics, constraints = constraints)
  r <- httr::POST(emsi_URL, body = body, encode = "json", agnitio_settings())
  Sys.sleep(1)
  r2 <- jsonlite::fromJSON(httr::content(r, "text", encoding="UTF-8"), simplifyDataFrame = TRUE)
  outputdata <- as.data.frame(r2$data$rows) %>% dplyr::as_tibble()
  colnames(outputdata) <- r2$data$name
  rm(r)
  rm(r2)
  outputdata <- outputdata %>% dplyr::mutate_if(is.character, as.factor)
  # if(ncol(outputdata[, colnames(outputdata) %in% metrics$as])>1) {
  #   outputdata[, colnames(outputdata) %in% metrics$as] <- apply(outputdata[, colnames(outputdata) %in% metrics$as], 2, function(x) as.numeric(x))
  # }
  return(outputdata)
}

#' Pull data from the Agnitio API
#'
#' @description Pulls data from the Emsi Agnitio API according to specified parameters and returns a prepared data frame for analysis.
#'
#' @param country The two-character country code.
#' @param content The Emsi Agnitio dataset description (e.g. "Occupation").
#' @param release The release or version identifier for the dataset (e.g. "2016.1").
#' @param constraints A list of dimensional constraints relevant to the dataset, each of which has been prepared through
#' \code{\link{dimmaker}} or \code{\link{CoW}}.
#' @param metrics A set of metrics through which to quantify the data, prepared through \code{\link{metricmaker}}.
#' @param limiter (optional) numerical limit on the dimensional size of your query, by default limited to 20,000
#' as the product of all the groups you're seeking to download - setting a higher number can make this work,
#' but think about rationing or saving the result.
#' @param brake (optional) changes the buffering time between queries to the API - default is set at 5 seconds,
#' and if you're planning to hammer the API with lots of queries, it's good to have this at least, maybe more.
#' @return A data frame of dimensions and metrics, with dimensions classified as factors and metrics as doubles.
#' @examples
#' met1 <- data.frame(names=c("Jobs.2016","Jobs.2022"), as=c("Jobs.2016","Jobs.2022"))
#' met1 <- metricmaker(met1)
#' area1 <- data.frame(name=c("Great Britain", "Wales"), code=c("GB", "WAL"))
#' areadim <- dimmaker("Area", area1)
#' occdim <- dimmaker("Occupation", data.frame(code="1"))
#' datapull("UK","Occupation","2016.1",list(CoW("A"),areadim,occsdim),met1)
#' @export
datapull <- function(country, content, release, constraints, metrics, limiter=2e4, brake=5) {
  dimN <- prod(unlist(purrr::map(constraints, ~ length(.$map))))
  if (dimN>limiter) stop(paste("Your query is asking for",formatC(dimN,big.mark=",",format="f",digits=0),
                               "dimensional cells, so you should think carefully about this!"))
  if (ncol(metrics) == 2) {
    outputdata <- pullquery(country, content, release, constraints, metrics)
  }
  if (ncol(metrics) > 2) {
    if (nrow(metrics[is.na(metrics$operation), ]) == 0) {
      outputdata <- pullquery(country, content, release, constraints, metrics)
    }
    if (nrow(metrics[is.na(metrics$operation), ]) > 0) {
      metrics1 <- metrics %>% dplyr::filter(is.na(operation)) %>% dplyr::select(-operation)
      metrics2 <- metrics %>% dplyr::filter(!is.na(operation))
      outputdata1 <- pullquery(country, content, release, constraints, metrics1)
      outputdata2 <- pullquery(country, content, release, constraints, metrics2)
      outputdata <- outputdata1 %>% dplyr::left_join(outputdata2)
      rm(outputdata1, outputdata2)
    }
  }
  return(outputdata)
  Sys.sleep(brake)
}

