CREATE TABLE PERSON (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE MOVIE (
    id TEXT NOT NULL,
    type TEXT,
    year INTEGER,
    duration INTEGER,
    rating FLOAT NOT NULL DEFAULT 0,
    votes INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE TABLE TITLE (
    movie TEXT NOT NULL,
    title TEXT NOT NULL,
    PRIMARY KEY (movie, title),
    FOREIGN KEY (movie) REFERENCES MOVIE(id)
);

/*
CREATE TABLE WORKER (
    movie TEXT,
    person TEXT NOT NULL,
    category TEXT NOT NULL,
    PRIMARY KEY (movie, person, category),
    FOREIGN KEY (movie) REFERENCES MOVIE(id),
    FOREIGN KEY (person) REFERENCES PERSON(id)
);
*/

CREATE TABLE DIRECTOR (
    movie TEXT,
    person TEXT NOT NULL,
    PRIMARY KEY (movie, person),
    FOREIGN KEY (movie) REFERENCES MOVIE(id),
    FOREIGN KEY (person) REFERENCES PERSON(id)
);