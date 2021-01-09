import sqlite3
from typing import Optional
import json
import logging
import sys
import requests

DATABASE_FILE = "db.sqlite"
ELASTIC_URL = "http://127.0.0.1:9200"
conn = sqlite3.connect(DATABASE_FILE)
logging.basicConfig(format="[%(asctime)s] %(message)s", level=logging.INFO, stream=sys.stdout)


class Movie:
    def __init__(self, _id, imdb_rating, genre, title, description, director, writer, writers):
        self.id = _id
        self.imdb_rating = None if imdb_rating == "N/A" else float(imdb_rating)
        self.genre = genre.split(", ")
        self.title = title
        self.description = None if description == "N/A" else description
        self.director = None if director == "N/A" else director.split(", ")

        actors_list = get_actors_list(movie_id=_id)
        writers_id = [writer] if writer else [item["id"] for item in json.loads(writers)]
        writers_list = get_writers_list(writers_id=writers_id)

        self.actors_names = get_names(peoples=actors_list)
        self.writers_names = get_names(peoples=writers_list)
        self.actors = actors_list
        self.writers = writers_list

    def to_json(self):
        return json.dumps(self.__dict__)

    def to_batch_query(self):
        index = json.dumps(
            {
                "index": {
                    "_index": "movies",
                    "_id": self.id
                }
            }
        )
        return f"{index}\n{self.to_json()}\n"


def get_movies_id() -> list[str]:
    """Получение списка id фильмов."""
    sql_query = "SELECT DISTINCT id FROM movies ORDER BY id ASC;"
    result = execute_select_query(select_query=sql_query)
    movies_id = []
    for item in result:
        movies_id.append(item[0])
    return movies_id


def get_movie(movie_id: str) -> Optional[Movie]:
    sql_query = f"SELECT id, imdb_rating, genre, title, plot, director, writer, writers " \
                f"FROM movies WHERE id='{movie_id}';"
    result = execute_select_query(select_query=sql_query)
    return Movie(*result[0]) if result else None


def get_movies_list(row_count: int = 1, offset: Optional[int] = None) -> list[Movie]:
    """Получение списка фильмов."""
    limit = f"{row_count}, {offset}" if offset else row_count
    sql_query = f"SELECT id, imdb_rating, genre, title, plot, director, writer, writers " \
                f"FROM movies ORDER BY id ASC LIMIT {limit};"
    result = execute_select_query(select_query=sql_query)
    movies = []
    for item in result:
        movies.append(Movie(*item))
    return movies


def execute_select_query(select_query: str) -> list[Optional[tuple]]:
    cursor = conn.cursor()
    cursor.execute(select_query)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_actors_list(movie_id: str) -> list[dict[str, Optional[str]]]:
    """
    Получение списка актёров по id фильма.
    Актёры тоже могуть быть N/A:
        id |name|
        ---|----|
        475|N/A |
    """
    sql_query = f"SELECT a.id, a.name " \
                f"FROM movie_actors ma JOIN actors a on ma.actor_id = a.id " \
                f"WHERE ma.movie_id='{movie_id}';"
    result = execute_select_query(select_query=sql_query)
    actors = []
    for item in result:
        actors.append({
            "id": str(item[0]),
            "name": None if item[1] == "N/A" else item[1]
        })
    return actors


# def get_names(peoples: list) -> str:
#     """Возвращает строку с перечислением имён, через запятую."""
#     names = []
#     for item in peoples:
#         if item["name"]:
#             names.append(item["name"])
#     return ", ".join(names) if names else None

def get_names(peoples: list) -> list[str]:
    """Возвращает список имён."""
    names = []
    for item in peoples:
        if item["name"]:
            names.append(item["name"])
    return names if names else None


def get_writers_list(writers_id: list[str]) -> list[dict[str, Optional[str]]]:
    """
    Получение списка сценаристов по списку их id.
    Сценаристы тоже могуть быть N/A:
        id                                      |name|
        ----------------------------------------|----|
        0b60f2f350405ece5ad187c866c105d7524104ba|N/A |
    """
    writers_id = ["'" + _id + "'" for _id in writers_id]
    sql_query = f"SELECT id, name FROM writers WHERE id in ({', '.join(writers_id)});"
    result = execute_select_query(select_query=sql_query)
    writers = []
    for item in result:
        writers.append({
            "id": item[0],
            "name": None if item[1] == "N/A" else item[1]
        })
    return writers


def create_batch(movies: list[Movie]) -> str:
    batch = ""
    for movie in movies:
        batch += movie.to_batch_query()
    return batch


def send_batch(batch: str):
    r = requests.post(
        url=f"{ELASTIC_URL}/_bulk?filter_path=items.*.error",
        headers={"Content-Type": "application/x-ndjson"},
        data=batch
    )
    logging.info(f"Response status code: {r.status_code}")
    logging.info(f"Response: {r.text}")


def run():
    logging.info("Start ETL script...")

    logging.info("Start getting movies ID's...")
    movies_id = get_movies_id()
    logging.info(f"Movies ID's get! Total: {len(movies_id)} ID's.")

    batch_size = 10
    logging.info(f"Batch size: {batch_size}.")

    movies = []
    batches_send = 0
    movies_left = len(movies_id)
    for _id in movies_id:
        movies.append(get_movie(movie_id=_id))
        movies_left -= 1
        if len(movies) == batch_size:
            batch = create_batch(movies=movies)
            send_batch(batch=batch)
            batches_send += 1
            logging.info(f"Batches send: {batches_send}")
            logging.info(f"Movies left: {movies_left}")
            movies.clear()
    logging.info(f"Last batch size: {len(movies)}")
    batch = create_batch(movies=movies)
    send_batch(batch=batch)
    batches_send += 1
    logging.info(f"Batches send: {batches_send}")
    logging.info("That's all!")


if __name__ == "__main__":
    run()
