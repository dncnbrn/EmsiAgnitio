#' @export
mapper <- function(mapping) {
  if (class(mapping$code)=="list") {
    mapping <- tidyr::unnest(mapping)
    map1 <- mapping %>% dplyr::group_by(name) %>% tidyr::nest()
    map2 <- purrr::map(map1$data, "code") %>% purrr::map(as.list)
    names(map2) <- map1$name
    return(map2)
  } else {
    map1 <- mapping %>% dplyr::group_by(name) %>% tidyr::nest()
    map2 <- purrr::map(map1$data, "code") %>% purrr::map(as.list)
    names(map2) <- map1$name
    return(map2)
  }
}

#' Specify dimension for an Agnitio API query
#'
#' @description  Takes a data frame of names and codes along a dimension and specifies them ready for an Emsi Agnitio data pull.
#'
#' @param dimension A named dimension required by the dataset being used for analysis (e.g. "Occupation").
#' @param mapping This may take the form of the atomic "asIdentity" if all possible individual items are required with
#' no relabelling, or else a data frame with two columns: \code{name} sets labels for \code{code} which refers to the
#' geographic, industry, occupation or other codes used to categorise data within Agnitio. Where one label is used for
#' multiple codes, \code{dimmaker} will merge them to form a hybrid category.
#'
#' If you want to pass a map with multiple codes to some of the labels, use a tibble with a list column for \code{code} and
#' then pass each label a sequence of the form \code{c("1211", "1212")}. If the data frame only has
#' the column \code{code} then this will be replicated as the name.
#'
#' @return A prepared list identifying the dimension and supporting mapping, ready to organise a data pull query.
#' @examples
#' mgrs <- data.frame(names=c("CEOs",
#'                            rep("Group of managers A",3),
#'                            rep("Group of managers B",4)),
#'                    codes=c("1115","1116","1121","1122","1123","1131","1132","1133"))
#' occs <- dimmaker("Occupation", mgrs)
#' @export
dimmaker <- function(dimension, mapping) {
  mapping$code <- as.character(mapping$code)
  if (is.atomic(mapping)) {
    if (mapping == "asIdentity") {
      list(dimensionName = dimension, asIdentity = TRUE)
    } else {
      message("No mapping or asIdentity proposed.")
    }
  } else {
    if (("name" %in% colnames(mapping))==FALSE) {
      mapping$name <- mapping$code
    }
    list(dimensionName = dimension, map = mapper(mapping))
  }
}

#' A short-hand function to map UK ClassOfWorker dimensions around one of four options
#'
#' @param option One of "E" (for Employee), "P" (for "Proprietor"), "A" (for "All" combined) or "S" (for
#' Employee and Proprietor separately.)
#' @return The necessary mapping for ClassOfWorker for inclusion in a data pull query.
#' @examples
#' CoW("E")
#' @export
CoW <- function(option) {
  if (option == "E") {
    mapClassCode <- list("1")
    mapClass <- list(Employees = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "P") {
    mapClassCode <- list("2")
    mapClass <- list(Proprietors = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "A") {
    mapClassCode <- list("1", "2")
    mapClass <- list(All = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "S") {
    mapClassCode1 <- list("1")
    mapClassCode2 <- list("2")
    mapClass <- list(Employees = mapClassCode1, Proprietors = mapClassCode2)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  return(ClassOfWorker)
}

#' A short-hand function to map US ClassOfWorker dimensions around one different options
#'
#' @param option One of "E" (for all employees, QCEW and non-QCEW),
#' "Q" (for QCEW Employee), "N" (for non-QCEW Employee),
#' "S" (for all self-employed), "SE1" (for self-employed but not extended proprietors),
#' "SE2" (for extended proprietors ). Finally, "A" gives all employed and self-employed,
#' and "S" gives all employed and self-employed broken down.
#' @return The necessary mapping for ClassOfWorker for inclusion in a data pull query.
#' @examples
#' CoW("E")
#' @export
usCoW <- function(option) {
  if (option == "A") {
    mapClassCode <- list("1","2","3","4")
    mapClass <- list(`All` = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "E") {
    mapClassCode <- list("1","2")
    mapClass <- list(`Employees` = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "Q") {
    mapClassCode <- list("1")
    mapClass <- list(`QCEW employees` = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "N") {
    mapClassCode <- list("2")
    mapClass <- list(`non-QCEW employees` = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "SE") {
    mapClassCode <- list("3","4")
    mapClass <- list(`Self-employed` = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "SE1") {
    mapClassCode <- list("3", "2")
    mapClass <- list(`Self-employed not extended proprietors` = mapClassCode)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  if (option == "S") {
    mapClassCode1 <- list("1","2")
    mapClassCode2 <- list("3","4")
    mapClass <- list(Employees = mapClassCode1, `Self-employed` = mapClassCode2)
    ClassOfWorker <- list(dimensionName = "ClassOfWorker", map = mapClass)
  }
  return(ClassOfWorker)
}
