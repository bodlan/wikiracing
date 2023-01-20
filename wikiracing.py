import psycopg2
import psycopg2.extras
import wikipedia
import networkx as nx
import pandas as pd
from datetime import timedelta
from collections import deque
from typing import List


requests_per_minute = 100
links_per_page = 200
wikipedia.set_lang("uk")
wikipedia.set_rate_limiting(True, min_wait=timedelta(minutes=1 / requests_per_minute))
table_name = "wiki_links"
max_depth = 4


class WikiRacer:
    def __init__(self):
        self._conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres")
        self.db_data = None
        self._create_table()
        self.graph = nx.DiGraph()

    def evaluate_db(self):
        """
        Топ 5 найпопулярніших статей (ті що мають найбільшу кількість посилань на себе)
        Топ 5 статей з найбільшою кількістю посилань на інші статті
        """
        self.set_update_graph_from_db_data()
        top_articles = sorted(self.graph.out_degree, key=lambda x: x[1], reverse=True)[:5]
        print("Топ 5 статей з найбільшою кількістю посилань на інші статті: ", top_articles)
        top_links = sorted(self.graph.in_degree, key=lambda x: x[1], reverse=True)[:5]
        print("Топ 5 найпопулярніших статей (ті що мають найбільшу кількість посилань на себе):", top_links)
        return top_articles, top_links

    def get_average_article_links(self, article: str) -> float:
        """
        Для заданної статті знайти середню кількість потомків другого рівня
        :return: середня кількість потомків другого рівня
        """
        self.set_update_graph_from_db_data()
        if article not in self.graph.nodes:
            print("No article {} in db".format(article))
            return 0
        depth = 2
        try:
            paths = [
                n
                for n in nx.single_source_shortest_path(self.graph, article, cutoff=depth).values()
                if len(n) == depth + 1
            ]
            number_of_nodes = len(paths)
            number_of_predecessors = len(set([n[depth - 1] for n in paths]))
            average = number_of_nodes / number_of_predecessors
            print("Cередня кількість потомків другого рівня для статті {}: {}".format(article, average))
            return average
        except ZeroDivisionError:
            print("Too much depth exceeded for article {} and depth = {}".format(article, depth))
            return 0
        except Exception as e:
            print("Exception whule getting average article successors:", e)
            raise e

    def get_routes(self, n: int):
        """
        Запит, що має параметр - N, повертає до п’яти маршрутів переходу довжиною N.
        Сторінки в шляху не мають повторюватись.
        :return:
        """
        self.set_update_graph_from_db_data()
        paths = []
        try:
            for src in self.graph.nodes:
                for trg in self.graph.nodes:
                    paths.extend(
                        [path for path in nx.all_simple_paths(self.graph, src, trg, cutoff=n) if len(path) == n]
                    )
                    if len(paths) >= 5:
                        paths = paths[:5]
                        print("П’ять маршрутів переходу довжиною {}: {}".format(n, paths))
                        return paths
        except Exception as e:
            print("Exception while getting routes:", e)
            return []

    def find_path(self, start: str, finish: str) -> List[str]:
        # check if articles exist
        try:
            wikipedia.page(start, auto_suggest=False)
            wikipedia.page(finish, auto_suggest=False)
        except (wikipedia.exceptions.DisambiguationError, wikipedia.PageError) as e:
            print("Can't find article in wikipedia:", e)
            raise e
        except Exception as e:
            print("Something went wrong while looking for article:", e)
            raise e
        path = self.get_shortest_path(start, finish)
        return path

    def set_update_graph_from_db_data(self):
        self._get_data_from_db()
        if not self.db_data.empty:
            data = self.db_data.explode("links")
            for title, links in zip(data["title"], data["links"]):
                self.graph.add_edge(title, links)

    def _create_table(self):
        with self._conn.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS {}("
                "id SERIAL PRIMARY KEY,"
                "title TEXT UNIQUE,"
                "links TEXT[{}] "
                ");".format(table_name, links_per_page)
            )
            self._conn.commit()

    def _get_data_from_db(self):
        try:
            sql = "select title, links from {}".format(table_name)
            self.db_data = pd.read_sql_query(sql, self._conn)
        except Exception as e:
            print("Exception while getting data from db:", e)
            raise e

    def _write_to_db(self, title: str, links: list):
        with self._conn.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO {}  (title, links) " "VALUES (%s, %s) ON CONFLICT DO NOTHING".format(table_name),
                    (title, links),
                )
                self._conn.commit()
            except Exception as e:
                print("Exception while writing to db:", e)
                raise e

    def get_shortest_path(self, start: str, finish: str) -> List[str]:
        print("Getting shortest path from {} to {}".format(start, finish))
        empty_nodes_q = deque()
        self.set_update_graph_from_db_data()
        if nx.is_empty(self.graph):
            empty_nodes_q.append(start)
        else:
            try:
                if not nx.has_path(self.graph, start, finish):
                    raise Exception
                return nx.shortest_path(self.graph, start, finish)
            except Exception:
                print("Don't have path in db")
            try:
                reachable_sorted_nodes = [
                    n[-1] for n in list(nx.single_source_shortest_path(self.graph, start, max_depth).values())
                ][1:]
                empty_nodes_q.extend(reachable_sorted_nodes)
            except Exception:
                empty_nodes_q.append(start)
        depth_count = 1
        while depth_count < max_depth:
            # making a distinct list of queue and keeping order
            list_q = sorted(set(list(empty_nodes_q)), key=list(empty_nodes_q).index)
            # print("len:", len(list_q), "Q:", list_q)
            for item in list_q:
                empty_nodes_q.popleft()
                try:
                    if any(True for _ in self.graph.successors(item)):
                        continue
                except nx.NetworkXError:
                    pass
                # print("Item:", item)
                item_links = list(set(self.retrieve_titles(item)))
                # check if titles found else continue
                if not item_links:
                    continue
                self._write_to_db(item, item_links)
                if finish in item_links:
                    self.graph.add_edge(item, finish)
                    return nx.shortest_path(self.graph, start, finish)
                for title in item_links:
                    empty_nodes_q.append(title)
                    self.graph.add_edge(item, title)
            depth_count += 1
        return []

    @staticmethod
    def retrieve_titles(page_title: str) -> List[str]:
        try:
            page = wikipedia.page(page_title, auto_suggest=False)
            links = page.links
            if page_title in links:
                links.remove(page_title)
            return links[:links_per_page]
        except (wikipedia.exceptions.DisambiguationError, wikipedia.PageError):
            print("Exception while retrieving page titles for page {}".format(page_title))
            return []

    def __del__(self):
        self._conn.close()
