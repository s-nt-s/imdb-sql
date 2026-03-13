DROP TABLE IF EXISTS EXTRA;

CREATE TABLE EXTRA (
    movie TEXT,
    filmaffinity INTEGER,
    wikipedia TEXT,
    countries TEXT,
    FOREIGN KEY (movie) REFERENCES MOVIE(id)
)
;