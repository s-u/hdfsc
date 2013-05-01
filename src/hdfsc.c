#include <string.h>
#include <stdlib.h>

#include <Rinternals.h>
#include <R_ext/Connections.h>
#if ! defined(R_CONNECTIONS_VERSION) || R_CONNECTIONS_VERSION != 1
#error "Unsupported connections API version"
#endif

typedef struct hdfsc {
    SEXP jpath;
    SEXP hif;
    SEXP buf;
    int  bpos, bsize;
} hdfsc_t;

static void hdfsc_close(Rconnection c) {
    hdfsc_t *hc = (hdfsc_t*) c->private;
    if (hc) {
	if (hc->buf != R_NilValue) {
	    R_ReleaseObject(hc->buf);
	    hc->bsize = hc->bpos = 0;
	}
	eval(PROTECT(lang2(install(".HDFS.close"), hc->hif)), R_GlobalEnv);
	UNPROTECT(1);
	c->isopen = 0;
    }
}

static Rboolean hdfsc_open(Rconnection c) {
    hdfsc_t *hc = (hdfsc_t*) c->private;
    if (hc) {
	if (c->isopen)
            Rf_error("connection is already open");
	if (c->mode[0] == 'r' && c->mode[1] != '+') {
	    hc->hif = eval(PROTECT(lang3(install(".HDFS.open"), hc->jpath, mkString(c->mode))), R_GlobalEnv);
	    setAttrib(hc->jpath, install("hif"), hc->hif);
	    UNPROTECT(1);
	    c->text = (c->mode[1] == 'b') ? FALSE : TRUE;
	    c->canwrite = FALSE;
	    c->canread = TRUE;
	} else Rf_error("Sorry, unsupported mode");
	c->isopen = TRUE;
	return TRUE;
    }
    return FALSE;
}

static size_t hdfsc_read(void *buf, size_t sz, size_t ni, Rconnection c) {
    hdfsc_t *hc = (hdfsc_t*) c->private;
    size_t len, req = sz * ni;
    if (!hc) Rf_error("invalid HDFS connection");
    /* all in the buffer */
    if (hc->bsize - hc->bpos <= req) {
	memcpy(buf, RAW(hc->buf) + hc->bpos, req);
	hc->bpos += req;
	return ni;
    }
    /* has buffer ? */
    if (hc->bsize) {
	if (hc->bpos < hc->bsize) { /* some content ... */
	    len = (hc->bsize - hc->bpos) / sz;
	    if (len) {
		len *= sz;
		memcpy(buf, RAW(hc->buf) + hc->bpos, len);
		hc->bpos += len;
		buf = (void*)(((char*) buf) + len);
		req -= len;
		if (hc->bpos > hc->bsize) /* fraction of ni left */
		    Rf_error("Reader has requested items of size %d but an item crosses block boundary, aborting", sz);
		/* FIXME: we cannot keep anything left over in the buffer, because there will be a new buffer for next read */
	    }
	}
    } /* WARNING: the above adjusts req but *not* ni so do NOT use ni below this line */
    if (hc->buf != R_NilValue) R_ReleaseObject(hc->buf);
    hc->buf = eval(PROTECT(lang2(install(".HDFS.read"), hc->hif)), R_GlobalEnv);
    hc->bsize = LENGTH(hc->buf);
    hc->bpos = 0;
    if (req > hc->bsize)
	req = (hc->bsize / sz)  * sz;
    memcpy(buf, RAW(hc->buf) + hc->bpos, req);
    hc->bpos += req;
    if (hc->bpos < hc->bsize) /* left-overs, need to keep the buffer */
	R_PreserveObject(hc->buf);
    else {
	hc->bpos = hc->bsize = 0;
	hc->buf = R_NilValue;
    }
    UNPROTECT(1);
    return req / sz;
}

static int hdfsc_fgetc(Rconnection c) {
    hdfsc_t *hc = (hdfsc_t*) c->private;
    unsigned char x[2];
    
    if (!hc) Rf_error("invalid HDFS connection");
    if (hc->bpos + 1 < hc->bsize) /* the fast way */
	return ((unsigned char*)RAW(hc->buf))[hc->bpos++];
    /* either need read (no buffer) or cleanup (last byte) */
    if (hdfsc_read(x, 1, 1, c) != 1) return -1;
    return x[0];
}

static size_t hdfsc_write(const void *buf, size_t sz, size_t ni, Rconnection c) {
    Rf_error("Sorry, currently unsupported");
    return 0;
}

SEXP mk_hdfs_conn(SEXP sPath, SEXP sMode, SEXP hPath) {
    Rconnection con;
    hdfsc_t *cc;
    SEXP rc;
    if (TYPEOF(sPath) != STRSXP || LENGTH(sPath) != 1)
        Rf_error("invalid path specification");
    if (TYPEOF(sMode) != STRSXP || LENGTH(sMode) != 1)
        Rf_error("invalid mode specification");    
    rc = R_new_custom_connection(CHAR(STRING_ELT(sPath, 0)),
                                 CHAR(STRING_ELT(sMode, 0)),
                                 "HDFS", &con);
    PROTECT(rc);
    cc = malloc(sizeof(hdfsc_t));
    if (!cc) Rf_error("cannot allocate HDFS private context");
    setAttrib(rc, install("hpath"), hPath); /* easy way to protect hPath even though it's inside the private part */
    cc->jpath = hPath;
    cc->hif = R_NilValue;
    cc->buf = R_NilValue;
    cc->bpos = cc->bsize = 0;
    con->private = cc;
    con->canseek = TRUE;
    con->blocking = TRUE;
    con->open = hdfsc_open;
    con->close = hdfsc_close;
    con->read = hdfsc_read;
    con->fgetc = hdfsc_fgetc;
    con->write = hdfsc_write;
    if (con->mode[0] && !con->open(con)) /* auto-open */
        Rf_error("cannot open the connection");
    UNPROTECT(1);
    return rc;
}
