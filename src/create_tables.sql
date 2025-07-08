USE topazsofer;

-- Create Movies Table
CREATE TABLE Movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year INT,
    release_date DATE,
    runtime INT,
    tagline TEXT,
    overview TEXT,
    popularity FLOAT,
    budget BIGINT,
    revenue BIGINT,
    INDEX idx_popularity (popularity),
    INDEX idx_release_year (release_year),
    INDEX idx_budget (budget),
    FULLTEXT INDEX (overview),
    FULLTEXT INDEX (title)
);

CREATE TABLE MoviesImdb (
	movie_id INT PRIMARY KEY,
    imdb_id VARCHAR(15)
);

-- Create Genres Table
CREATE TABLE Genres (
    genre_id INT PRIMARY KEY,
    genre_name VARCHAR(100) UNIQUE
);

-- Create MovieGenres Table
CREATE TABLE MovieGenres (
    movie_id INT,
    genre_id INT,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
);

-- Create Actors Table
CREATE TABLE Actors (
    actor_id INT PRIMARY KEY,
    actor_name VARCHAR(255) UNIQUE
);

-- Create MovieActors Table
CREATE TABLE MovieActors (
    movie_id INT,
    actor_id INT,
    PRIMARY KEY (movie_id, actor_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
    FOREIGN KEY (actor_id) REFERENCES Actors(actor_id)
);

-- Create Directors Table
CREATE TABLE Directors (
    director_id INT PRIMARY KEY,
    director_name VARCHAR(255) UNIQUE
);

-- Create MovieDirectors Table
CREATE TABLE MovieDirectors (
    movie_id INT,
    director_id INT,
    PRIMARY KEY (movie_id, director_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
    FOREIGN KEY (director_id) REFERENCES Directors(director_id)
);

-- Create Production Companies Table
CREATE TABLE ProductionCompanies (
    company_id INT PRIMARY KEY,
    company_name VARCHAR(255) UNIQUE
);

-- Create MovieProductionCompanies Table
CREATE TABLE MovieProductionCompanies (
    movie_id INT,
    company_id INT,
    PRIMARY KEY (movie_id, company_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
    FOREIGN KEY (company_id) REFERENCES ProductionCompanies(company_id)
);
