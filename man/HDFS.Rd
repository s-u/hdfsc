\name{HDFS}
\alias{HDFS}
\title{
  HDFS (Hadoop Distributed File System) connections
}
\description{
  \code{HDFS} creates a new HDFS connection that can be used
  to read files from HDFS.
}
\usage{
  HDFS(path, mode = "r", fs, buffer = 8388608L)
}
\arguments{
  \item{path}{path of the file in HDFS}
  \item{mode}{mode to use for auto-open}
  \item{fs}{optional, FileSystem object}
  \item{buffer}{buffer size to use for pre-loading the data}
}
\details{
  \code{HDFS} creates a connection that can be used for reading files
  from HDFS (currently write is unimplemented). The connection can be
  opened either for text reading \code{"r"} or for binary reads
  \code{"rb"}.

  The \code{buffer} parameter specifies the size (in bytes) of the
  buffer used to read the data.
}
\value{
  HDFS connection
}
%\references{
%}
\author{
  Simon Urbanek
}
%\note{
%}
%\seealso{
%}
\examples{
\dontrun{
f <- HDFS("/projects/foo/bar", "r")
d <- read.table(f, stringsAsFactors=FALSE)
close(f)
}
}
\keyword{connection}
