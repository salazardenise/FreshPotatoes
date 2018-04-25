const sqlite = require('sqlite'),
      sqlite3 = require('sqlite3'),
      Sequelize = require('sequelize'),
      request = require('request'),
      express = require('express'),
      app = express();

const { PORT=3000, NODE_ENV='development', DB_PATH='./db/database.db' } = process.env;

const URL_THIRD_PARTY = 'http://credentials-api.generalassemb.ly/4576f55f-c427-4cfc-a11c-5bfe914ca6c1?films=';
const MIN_NUM_REVIEWS = 5;
const MIN_REVIEW_AVG = 4.0

// START SERVER
Promise.resolve()
  .then(() => app.listen(PORT, () => console.log(`App listening on port ${PORT}`)))
  .catch((err) => { if (NODE_ENV === 'development') console.error(err.stack); });

// CONNECT TO DATABASE
const db = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READONLY, (err) => {
  if (err) {
    console.error(err.message);
  }
  console.log('Connected to the database.');
});

// ROUTES
app.get('/films/:id/recommendations', getFilmRecommendations);
app.get('*', function(req, res) {
  console.log('missing route');
  res.status(404).json({ 'message' : 'Invalid Route'});
});

// ROUTE HANDLER
function getFilmRecommendations(req, res) {
  // main steps:
  // STEP 1: Validate id of filmEntry
  // STEP 2: Get candidateFilms of same genre and near release date (within +/-15 years)
  // STEP 3: From that list, get finalFilms that pass reviews and ratings test
  getFilmEntry(req.params.id)
    .then((filmEntry) => {
      console.log('Film genre id: ' + filmEntry.genre_id);
      return getSimilarFilms(filmEntry);
    }).then((candidateFilms) => {
      console.log('Candidate reccomendations: ' + candidateFilms.length);
      return filterCandidateFilmsByReviews(candidateFilms);
    }).then((finalFilms) => {
      console.log('Number of final recomendations: ' + finalFilms.length);
      res.status(200).send(finalFilms);
    }).catch((errMessage) => {
      console.error('Promise rejected: ' + errMessage);
      res.status(422).json({ 'message' : errMessage });
    });
}


// HELPER FUNCTIONS
// Query the database using a film id
// Return a promise that resolves on the film entry corresponding to the film id
function getFilmEntry(filmId) {
  return new Promise((resolve, reject) => {
    let sqlId = `SELECT *
                 FROM films
                 WHERE id=${filmId}`;
    db.get(sqlId, [], (err, row) => {
      if (err) {
        reject(err.message);
      }
      if (!row) {
        reject(filmId + ' key missing');
      } else {
        resolve(row);
      }
    });
  });
}

// Query the database using a genre id and date with +/-15 years
// Return a promise that resolves on all the film entries for those criteria
function getSimilarFilms(filmEntry) {
  return new Promise((resolve, reject) => {
    var genreId = filmEntry.genre_id;
    var releaseDate = filmEntry.release_date;
    let sql = `SELECT *
               FROM films
               WHERE genre_id=${genreId}
               AND release_date
               BETWEEN datetime('${releaseDate}','-15 years')
               AND datetime('${releaseDate}','+15 years')
               ORDER BY id DESC`;
    db.all(sql, [], (err, rows) => {
      if (err) {
        reject(err.message);
      }
      if (rows.length == 0) {
        reject('Zero rows');
      } else {
        resolve(rows);
      }
    });
  });
}


// Filter candidate films based on reviews criteria
// i.e. film has at least 5 reviews and an average reviews rating of at least 4.0
function filterCandidateFilmsByReviews(candidateFilms) {
  return new Promise((resolve, reject) => {
    // Gather all the reviews for the candidate films
    Promise.all(candidateFilms.map((film) => {
      return getReviews(film.id)
               .then((reviews) => {
                 if (reviews.length < MIN_NUM_REVIEWS) {
                   return false;
                 }
                 var ratingSum = reviews.reduce((sum, review) => sum + review.rating, 0);
                 return ratingSum/reviews.length >= MIN_REVIEW_AVG;
               });
    }))
    // filter the films based on the reviews now
    .then((results) => {
      resolve(candidateFilms.filter((film, index) => {
        return results[index];
      }));
    });
  });
}

// Query the third party film review database using a film id
// Return a promise that resolves on the reviews for the film
function getReviews(filmId) {
  return new Promise((resolve, reject) => {
    request(URL_THIRD_PARTY + filmId, function(error, response, body) {
      if (error) {
        reject(error);
      }
      try {
        resolve(JSON.parse(body)[0].reviews);
      } catch(e) {
        reject(e);
      }
    });
  });
}

module.exports = app;
