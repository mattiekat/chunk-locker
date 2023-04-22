CREATE TABLE archive
(
    id INTEGER PRIMARY KEY,
    -- a uniquely identifying archive name
    name  TEXT UNIQUE NOT NULL,
    -- name of the machine which is being backed up
    machine_name  TEXT NOT NULL,
    -- a random integer indicates this archive is locked, null indicates unlocked
    write_lock    INTEGER
) STRICT;

CREATE TABLE archive_path
(
    archive_id INTEGER NOT NULL,
    -- a root path of the given archive
    path       TEXT    NOT NULL,

    FOREIGN KEY (archive_id)
        REFERENCES archive (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
) STRICT;

CREATE TABLE snapshot
(
    id                INTEGER PRIMARY KEY,
    archive_id        INTEGER NOT NULL,
    -- the hash algorithm used for this snapshot
    hash_type         TEXT    NOT NULL,
    -- the encryption algorithm used for this snapshot
    encryption_type   TEXT    NOT NULL,
    -- the chunking algorithm used for this snapshot
    chunking_type     TEXT    NOT NULL,
    -- version of the software that made this snapshot
    software_version  INTEGER NOT NULL,
    -- epoch time when this snapshot was started
    time_started      INTEGER NOT NULL,
    -- epoch time when this snapshot was finished (null if it is/was not finished)
    time_finished     INTEGER,
    -- total number of files in the FS for this snapshot; may not be accurate if
    -- `time_finished` is null.
    total_files       INTEGER,
    -- total number of directories in the FS for this snapshot; may not be accurate if
    -- `time_finished` is null.
    total_directories INTEGER,
    -- total number of file bytes in the FS for this snapshot; may not be the true size
    -- on disk for compressed FS formats; may not be accurate if `time_finished` is
    -- null.
    uncompressed_size INTEGER,
    -- total number of files bytes in the FS for this snapshot after being compressed
    -- and encrypted; may not be accurate if `time_finished` is null.
    compressed_size   INTEGER,

    FOREIGN KEY (archive_id)
        REFERENCES archive (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
) STRICT;

CREATE TABLE filesystem_entry
(
    id                  INTEGER PRIMARY KEY,
    snapshot_id         INTEGER NOT NULL,
    -- directory this entry is in, null if it is a root
    parent_directory_id INTEGER,
    -- path fragment; the current name of the dir/file not including any parents; to
    -- construct a full path you need build it from this partial path and all parent
    -- directories' partial paths; for files, this will include the file extension.
    partial_path        TEXT    NOT NULL,
    -- unix permission bits
    permissions         INTEGER NOT NULL,
    -- unix owner id
    owner               INTEGER NOT NULL,
    -- unix group id
    "group"             INTEGER NOT NULL,
    -- epoch time of when this FS entry was created on the FS
    creation_time       INTEGER NOT NULL,
    -- epoch time of when this FS entry was last modified on the FS
    modification_time   INTEGER NOT NULL,

    FOREIGN KEY (snapshot_id)
        REFERENCES snapshot (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    FOREIGN KEY (parent_directory_id)
        REFERENCES filesystem_entry (id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
) STRICT;

CREATE TABLE file
(
    id                  INTEGER PRIMARY KEY,
    filesystem_entry_id INTEGER NOT NULL,
    -- compression algorithm used
    compression_type    TEXT    NOT NULL,
    -- size of the file before compression and encryption
    original_size       INTEGER NOT NULL,
    -- size of the compressed and encrypted file
    stored_size         INTEGER NOT NULL,
    -- checksum of the entire file, acts as data integrity check to ensure there has
    -- been no corruption once things are put back together on the other end.
    checksum            BLOB    NOT NULL,
    FOREIGN KEY (filesystem_entry_id)
        REFERENCES filesystem_entry (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
) STRICT;

CREATE TABLE file_relation
(
    file_id       INTEGER NOT NULL,
    chunk_id      INTEGER NOT NULL,
    -- ordering of this chunk in the file, e.g. 4 indicates it is the 5th chunk in the
    -- file coming after 0, 1, 2, and 3.
    chunk_ordinal INTEGER NOT NULL,

    FOREIGN KEY (file_id)
        REFERENCES file (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    FOREIGN KEY (chunk_id)
        REFERENCES chunk (id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
) STRICT;

CREATE TABLE chunk
(
    id       INTEGER PRIMARY KEY,
    -- size of this chunk
    size     INTEGER     NOT NULL,
    -- hash of the compressed and encrypted blob, this is what is used for
    -- deduplication.
    hash     BLOB UNIQUE NOT NULL,
    -- optional checksum that is dependent on the storage location and can be used to
    -- check integrity of the chunk data without having to download the chunk. Some remotes
    -- will just use the hash.
    checksum BLOB
) STRICT;
