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

.jtoPOSIX <- function(x) .POSIXct(ifelse( x == 0, NA, x / 1000))
.df <- function(...) { l <- list(...); attr(l, "row.names") <- .set_row_names(length(l[[1L]])); class(l) <- "data.frame"; l }
                       
hdfs.info <- function(paths, fs) {
    if (missing(fs)) fs <- volatiles$fs
    if (is.jnull(fs)) stop("invalid file system")
    paths <- as.character(paths)
    if (length(paths) > 1) do.call("rbind", lapply(paths, hdfs.info, fs)) else {
        st <- .jcall(fs, "Lorg/apache/hadoop/fs/FileStatus;", "getFileStatus", .jnew("org/apache/hadoop/fs/Path", paths))
        .df(size = .jcall(st, "J", "getLen"),
            isdir = .jcall(st, "Z", "isDir"),
            mode = as.octmode(.jcall(.jcall(st, "Lorg/apache/hadoop/fs/permission/FsPermission;", "getPermission"), "T", "toShort")),
            mtime = .jtoPOSIX(.jcall(st, "J", "getModificationTime")),
            ctime = NA, # there is no CTIME in HDFS ...
            atime = .jtoPOSIX(.jcall(st, "J", "getAccessTime")),
            uid = NA, gid = NA, ## FIXME: we cannot get uid/gid directly, but we cound try to map names through a syscall
            uname = .jcall(st, "Ljava/lang/String;", "getOwner"),
            grname= .jcall(st, "Ljava/lang/String;", "getGroup"),
                   blocksize = .jcall(st, "J", "getBlockSize"),
            repl = .jcall(st, "T", "getReplication"))
    }
}

hdfs.rename <- function(from, to, fs) {
    if (missing(fs)) fs <- volatiles$fs
    if (is.jnull(fs)) stop("invalid file system")
    from <- as.character(from)
    to <- as.character(to)
    if (!identical(length(from), 1L) || !identical(length(to), 1L)) stop("to/from must be strings")
    .jcall(fs, "Z", "rename", .jnew("org/apache/hadoop/fs/Path", from), .jnew("org/apache/hadoop/fs/Path", to))
}

hdfs.home <- function(fs) {
    if (missing(fs)) fs <- volatiles$fs
    if (is.jnull(fs)) stop("invalid file system")
    .jcall(.jcall(fs, "Lorg/apache/hadoop/fs/Path;", "getHomeDirectory"), "Ljava/lang/String;", "toString")
}

hdfs.mkdir <- function(path, mode, fs) {
    if (missing(fs)) fs <- volatiles$fs
    if (is.jnull(fs)) stop("invalid file system")
    path <- as.character(path)
    if (!length(path)) return(logical(0))
    if (missing(mode)) {
        if (length(path) > 1) sapply(path, hdfs.mkdir, fs=fs) else .jcall(fs, "Z", "mkdirs", .jnew("org/apache/hadoop/fs/Path", path))
    } else {
        if (length(path) > 1) mapply(hdfs.mkdir, path, rep(as.character(mode), length.out=length(path)), fs) else {
            o <- as.octmode(mode)
            .jcall(fs, "Z", "mkdirs", .jnew("org/apache/hadoop/fs/Path", path), .jnew("org/apache/hadoop/fs/permission/FsPermission", .jshort(o)))
        }            
    }
}
