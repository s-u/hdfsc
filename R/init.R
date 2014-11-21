volatiles <- new.env(parent=emptyenv())
attr(volatiles, "name") <- "volatiles:hdfsc"

.init <- function() {
  hh <- Sys.getenv("HADOOP_HOME")
  if (!nzchar(hh)) hh <- Sys.getenv("HADOOP_PREFIX")
  hcmd <- file.path(hh, "bin", "hadoop")
  if (!nzchar(hh) || !file.exists(hcmd)) {
     hcmd <- Sys.which("hadoop")[1L]
     if (!nzchar(hcmd))
       stop("Cannot find working Hadoop home. Set HADOOP_PREFIX if in doubt.")
  }
  cp <- system(paste(shQuote(hcmd), "classpath"), intern=TRUE)
  if (!nzchar(cp)) cp <- Sys.getenv("HADOOP_CLASSPATH")
  if (!nzchar(cp)) stop("cannot determine Hadoop class path! Make sure hadoop works or set HADOOP_PREFIX and/or HADOOP_CLASSPATH")
  cp <- unlist(strsplit(cp, .Platform$path.sep, fixed=TRUE))

  ## the Java spec is brain-dead, it uses * to match exactly either of *.jar or *.JAR,
  ## so it's really painful to implement that (what should have been a simple glob..)
  if (any(grepl("\\*$", cp)))
    cp <- unlist(lapply(cp, function(o) if (grepl("\\*$", o)) Sys.glob(paste0(gsub("\\*$","",o),c('*.jar','*.JAR'))) else o))
  cp <- cp[nzchar(cp)] ## remove empty entries
  .jaddClassPath(cp)
  volatiles$cfg <- .jnew("org/apache/hadoop/conf/Configuration")
  .jcall(volatiles$cfg, "V", "setClassLoader", .jcast(.jclassLoader(), "java/lang/ClassLoader"))
  volatiles$fs <- .jcall("org/apache/hadoop/fs/FileSystem", "Lorg/apache/hadoop/fs/FileSystem;", "get", volatiles$cfg)
  volatiles$BYTE.TYPE <- .jfield("java/lang/Byte", "Ljava/lang/Class;", "TYPE")
}

.onLoad <- function(lib, pkg) {
  .jpackage(pkg, lib.loc = lib)
  .Call(set_eval_env, environment(.HDFS.open))

  if (!isTRUE(getOption("hdfsc.defer.init"))) .init()
}

HDFS <- function(path, mode="r", fs, buffer = 8388608L) {
  if (missing(fs)) fs <- volatiles$fs
  if (is.jnull(fs)) stop("invalid file system")
  path <- as.character(path)[1]
  hpath <- .jnew("org/apache/hadoop/fs/Path", path)
  bs <- as.integer(buffer)
  attr(hpath, "fs") <- fs
  attr(hpath, "buffer") <- .jcall("java/lang/reflect/Array", "Ljava/lang/Object;", "newInstance", volatiles$BYTE.TYPE, bs)
  attr(hpath, "buffer.size") <- bs
  .Call(mk_hdfs_conn, path, mode, hpath)
}

.HDFS.open <- function(hpath, mode) {
  if (mode == "r" || mode == "rb") {
    hif <- .jcall(attr(hpath, "fs"), "Lorg/apache/hadoop/fs/FSDataInputStream;", "open", hpath, attr(hpath, "buffer.size"))
    attr(hif, "buffer") <- attr(hpath, "buffer")
    attr(hif, "buffer.size") <- attr(hpath, "buffer.size")
    hif
  }
  else stop("Sorry, unsupported mode at this point")
}

.HDFS.read <- function(hif) {
  b <- attr(hif, "buffer")
  n <- .jcall(hif, "I", "read", .jcast(b, "[B"))
  ## this is undocumented in HDFS but read will return -1 on EOF
  if (n < 0) return(raw())
  if (n == attr(hif, "buffer.size")) return(.jevalArray(b))
  b2 <- .jcall("java/lang/reflect/Array", "Ljava/lang/Object;", "newInstance", volatiles$BYTE.TYPE, n)
  .jcall("java/lang/System", "V", "arraycopy", b, 0L, b2, 0L, n)
  .jevalArray(b2)
}

.HDFS.seek <- function(hif, pos)
  .jcall(hif, "V", "seek", hif, .jlong(pos))

.HDFS.close <- function(hif)
  .jcall(hif, "V", "close")
