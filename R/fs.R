hdfs.unlink <- function(x, recursive = TRUE, fs) {
    if (missing(fs)) fs <- volatiles$fs
    if (is.jnull(fs)) stop("invalid file system")
    fns <- as.character(x)
    res <- unlist(lapply(fns, function(path) .jcall(fs, "Z", "delete", .jnew("org/apache/hadoop/fs/Path", path), as.logical(recursive))))
    if (length(res) > 1) names(res) <- x
    res
}

hdfs.exists <- function(x, fs) {
    if (missing(fs)) fs <- volatiles$fs
    if (is.jnull(fs)) stop("invalid file system")
    fns <- as.character(x)
    res <- unlist(lapply(fns, function(path).jcall(fs, "Z", "exists", .jnew("org/apache/hadoop/fs/Path", path))))
    if (length(res) > 1) names(res) <- x
    res
}

hdfs.glob <- function(paths, dirmark=FALSE, fs) {
    if (missing(fs)) fs <- volatiles$fs
    if (is.jnull(fs)) stop("invalid file system")
    paths <- as.character(paths)
    if (length(paths) != 1) unlist(lapply(paths, hdfs.glob, dirmark, fs)) else {
        fl <- .jcall(fs, "[Lorg/apache/hadoop/fs/FileStatus;", "globStatus", .jnew("org/apache/hadoop/fs/Path", paths))
        unlist(lapply(fl, function(o) {
            p <- .jcall(o, "Lorg/apache/hadoop/fs/Path;", "getPath")
            s <- .jcall(p, "Ljava/lang/String;", "toString")
            if (dirmark && isTRUE(.jcall(o, "Z", "isDir"))) s <- paste0(s, "/")
            s
        }))
    }
}

hdfs.info <- function(paths, fs) {
    if (missing(fs)) fs <- volatiles$fs
    if (is.jnull(fs)) stop("invalid file system")
    paths <- as.character(paths)
    if (length(paths) > 1) do.call("rbind", lapply(paths, hdfs.info, fs)) else {
        st <- .jcall(fs, "Lorg/apache/hadoop/fs/FileStatus;", "getFileStatus", .jnew("org/apache/hadoop/fs/Path", paths))
        data.frame(size = .jcall(st, "J", "getLen"),
                   isdir = .jcall(st, "Z", "isDir"),
                   mode = NA, ## FIXME 
                   mtime = .POSIXct(.jcall(st, "J", "getModificationTime") / 1000),
                   ctime = NA,
                   atime = .POSIXct(.jcall(st, "J", "getAccessTime") / 1000),
                       uid = NA, gid = NA,
                   uname = .jcall(st, "Ljava/lang/String;", "getOwner"),
                   grname= .jcall(st, "Ljava/lang/String;", "getGroup"),
                   blocksize = .jcall(st, "J", "getBlockSize"),
                   repl = .jcall(st, "T", "getReplication"))
    }
}
