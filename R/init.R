.init <- function() {
  hadoop <- getOption("hadoop.cmd")
  if (is.null(hadoop)) hadoop <- "hadoop"
  cp <- system(paste(shQuote(hadoop), "classpath"), intern=TRUE)
  .jaddClassPath(unlist(strsplit(cp, .Platform$path.sep, fixed=TRUE)))
  volatiles$cfg <- .jnew("org/apache/hadoop/conf/Configuration")
  .jcall(volatiles$cfg, "V", "setClassLoader", .jcast(.jclassLoader(), "java/lang/ClassLoader"))
  volatiles$fs <- .jcall("org/apache/hadoop/fs/FileSystem", "Lorg/apache/hadoop/fs/FileSystem;", "get", volatiles$cfg)
  volatiles$BYTE.TYPE <- .jfield("java/lang/Byte", "Ljava/lang/Class;", "TYPE")
}

.onLoad <- function(lib, pkg) {
  .jpackage(pkg, lib.loc = lib)

  .imports <- parent.env(topenv())
  .i.par <- parent.env(.imports)
  volatiles <- new.env(parent=.i.par)
  attr(volatiles, "name") <- "volatiles:hdfsc"
  parent.env(.imports) <- volatiles

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
  if (mode == "r" || mode == "rb")
    .jcall(attr(hpath, "fs"), "Lorg/apache/hadoop/fs/FSDataInputStream;", "open", hpath, attr(path, "buffer.size"))
  else stop("Sorry, unsupported mode at this point")
}

.HDFS.read <- function(hif) {
  b <- attr(hif, "buffer")
  n <- .jcall(hif, "I", "read", .jcast(b, "[B"))
  if (n == attr(hif, "buffer.size")) return(.jevalArray(b))
  b2 <- .jcall("java/lang/reflect/Array", "Ljava/lang/Object;", "newInstance", volatiles$BYTE.TYPE, n)
  .jcall("java/lang/System", "V", "arraycopy", b, 0L, b2, 0L, n)
  .jevalArray(b2)
}

.HDFS.seek <- function(hif, pos)
  .jcall(hif, "V", "seek", hif, .jlong(pos))
