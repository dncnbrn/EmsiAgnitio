#' @export
pullquery <- function(country, content, release, constraints, metrics) {
  emsi_URL <- paste("http://agnitio.emsicloud.com/emsi.", country, ".", content, "/", release, sep = "")
  body <- list(metrics = metrics, constraints = constraints)
  r <- httr::POST(emsi_URL, body = body, encode = "json", agnitio_settings())
  Sys.sleep(1)
  r2 <- jsonlite::fromJSON(httr::content(r, "text", encoding="UTF-8"), simplifyDataFrame = TRUE)
  outputdata <- as.data.frame(r2$data$rows) %>% dplyr::tbl_df()
  colnames(outputdata) <- r2$data$name
  rm(r)
  rm(r2)
  outputdata <- outputdata %>% dplyr::mutate_if(is.character, as.factor)
  # if(ncol(outputdata[, colnames(outputdata) %in% metrics$as])>1) {
  #   outputdata[, colnames(outputdata) %in% metrics$as] <- apply(outputdata[, colnames(outputdata) %in% metrics$as], 2, function(x) as.numeric(x))
  # }
  return(outputdata)
}

#' @export
datapullcore <- function(country, content, release, constraints, metrics,brake=5) {
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
#' @param quota (optional) changes the limit per query to pass through to the API - default is 400 and if you ask for 500 or more you will get locked out.
#' @param brake (optional) changes the buffering time between queries to the API - default is set between 0 and 30 seconds, scaling depending on your demand.
#' @return A data frame of dimensions and metrics, with dimensions classified as factors and metrics as doubles.
#' @examples
#' met1 <- data.frame(names=c("Jobs.2016","Jobs.2022"), as=c("Jobs.2016","Jobs.2022"))
#' met1 <- metricmaker(met1)
#' area1 <- data.frame(name=c("Great Britain", "Wales"), code=c("GB", "WAL"))
#' areadim <- dimmaker("Area", area1)
#' occdim <- dimmaker("Occupation", data.frame(code="1"))
#' datapull("UK","Occupation","2016.1",list(CoW("A"),areadim,occsdim),met1)
#' @export
datapull <- function(country, content, release, constraints, metrics, quota, brake) {
  if (isFALSE(methods::hasArg(quota))) {
    quota <- 400
  }
  constnames <- purrr::map(constraints, "dimensionName")
  constnames_df <- tibble::enframe(constnames) %>% 
    tidyr::unnest(cols=value) %>% 
    dplyr::rename(dimensionName=value,
           group=name)
  constmap <- purrr::map(constraints, "map")
  order <- tibble::enframe(constmap) %>%
    dplyr::rename(group=name) %>%
    dplyr::mutate(name=purrr::map(value, names)) %>%
    tidyr::unnest(cols=c(value,name)) %>% 
    tidyr::unnest(cols=value) %>% 
    tidyr::unnest(cols=value) %>% 
    dplyr::rename(code=value) %>% 
    dplyr::left_join(constnames_df) %>% 
    dplyr::select(-group)
  combos <- expand.grid(purrr::map(constmap, names))
  colnames(combos) <- purrr::map_chr(constraints, "dimensionName")
  ration <- combos %>% 
    dplyr::mutate(count=seq(1,nrow(.),by=1),
           grp=ceiling(count/(quota/nrow(metrics)))) %>% 
    dplyr::select(-count) %>% 
    tidyr::gather(dimensionName,name,1:(ncol(.)-1)) %>% 
    dplyr::group_by(grp,dimensionName,name) %>% 
    dplyr::distinct() %>% 
    dplyr::ungroup()
  if (isFALSE(methods::hasArg(brake))) {
    brake <- ifelse(sqrt(length(unique(ration$grp))-1) > 30,
                    30,
                    sqrt(length(unique(ration$grp))-1))
  }
  message(paste("You have",
                  nrow(combos),
                  "dimension groupings and",
                  nrow(metrics),
                  "metrics and so this will take",
                  length(unique(ration$grp)),
                  "queries and at least",
                  ifelse(length(unique(ration$grp))*brake > 179,
                         paste(scales::comma(length(unique(ration$grp))*brake/60),
                          "minutes"),
                         paste(round(length(unique(ration$grp))*brake,digits=0),
                               "seconds"))))
  result <- dplyr::left_join(ration, order) %>% 
    dplyr::group_by(grp,dimensionName) %>% 
    tidyr::nest() %>% 
    dplyr::mutate(dims=purrr::map2(dimensionName, data, dimmaker)) %>% 
    dplyr::ungroup() %>% 
    dplyr::select(-dimensionName,-data) %>% 
    dplyr::group_by(grp) %>% 
    tidyr::nest()
  dp <- function(dm) {
    datapullcore(country,content,release,
             purrr::flatten(dm),
             metrics,
             brake)
  }
  result %>% 
    dplyr::mutate(pullit=purrr::map(data, dp)) %>% 
    dplyr::ungroup() %>% 
    dplyr::select(-grp,-data) %>% 
    tidyr::unnest(col=pullit) %>% 
    dplyr::group_by_at(dplyr::vars(dplyr::one_of(constnames_df$dimensionName))) %>%
    dplyr::slice(1L) %>% 
    dplyr::ungroup()
}
